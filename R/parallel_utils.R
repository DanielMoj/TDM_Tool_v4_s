# R/parallel_utils.R
# Parallelisierungs-Utilities für TDMx
# Adaptive Worker-Verwaltung und plattformübergreifende Parallelisierung

# Dependencies check
if (!requireNamespace("parallel", quietly = TRUE)) {
  stop("Package 'parallel' required. Install with: install.packages('parallel')")
}
if (!requireNamespace("future", quietly = TRUE)) {
  warning("Package 'future' recommended for async operations. Install with: install.packages('future')")
}
if (!requireNamespace("progressr", quietly = TRUE)) {
  warning("Package 'progressr' recommended for progress tracking. Install with: install.packages('progressr')")
}

# ============================================================================
# Core Parallel Configuration
# ============================================================================

#' Ermittelt optimale Anzahl von Worker-Prozessen
#' 
#' @param max_cores Maximale Anzahl Cores (NULL = auto)
#' @param reserve_cores Anzahl Cores für System reservieren (default: 1)
#' @param task_type Art der Aufgabe ("cpu", "memory", "io")
#' @return Optimale Anzahl Worker
#' @export
get_optimal_workers <- function(max_cores = NULL, 
                                reserve_cores = 1,
                                task_type = "cpu") {
  
  available_cores <- parallel::detectCores(logical = FALSE)
  
  # Plattform-spezifische Anpassungen
  if (.Platform$OS.type == "windows") {
    # Windows: konservativer wegen Overhead
    optimal <- available_cores - reserve_cores
  } else {
    # Unix/Mac: kann mehr auslasten
    optimal <- available_cores - reserve_cores
  }
  
  # Task-spezifische Anpassungen
  optimal <- switch(task_type,
    "memory" = min(optimal, 4),  # Memory-intensive: weniger Worker
    "io" = min(optimal, 6),      # I/O-bound: mittlere Anzahl
    "cpu" = optimal               # CPU-bound: volle Auslastung
  )
  
  # Begrenzung durch max_cores
  if (!is.null(max_cores)) {
    optimal <- min(optimal, max_cores)
  }
  
  # Mindestens 1 Worker
  return(max(1, optimal))
}

#' Erstellt plattformspezifischen Cluster
#' 
#' @param n_cores Anzahl Cores
#' @param type Cluster-Typ (NULL = auto)
#' @return Cluster-Objekt
#' @export
make_adaptive_cluster <- function(n_cores = NULL, type = NULL) {
  
  if (is.null(n_cores)) {
    n_cores <- get_optimal_workers()
  }
  
  # Auto-detect cluster type
  if (is.null(type)) {
    type <- if (.Platform$OS.type == "windows") "PSOCK" else "FORK"
  }
  
  # Create cluster with error handling
  cl <- tryCatch({
    if (type == "FORK" && .Platform$OS.type != "windows") {
      parallel::makeForkCluster(n_cores)
    } else {
      parallel::makePSOCKcluster(n_cores)
    }
  }, error = function(e) {
    warning(sprintf("Failed to create %s cluster, falling back to PSOCK: %s", 
                   type, e$message))
    parallel::makePSOCKcluster(n_cores)
  })
  
  # Set cluster options
  parallel::clusterEvalQ(cl, {
    # Lade benötigte Packages in Worker
    suppressPackageStartupMessages({
      if (requireNamespace("deSolve", quietly = TRUE)) library(deSolve)
      if (requireNamespace("MASS", quietly = TRUE)) library(MASS)
    })
  })
  
  return(cl)
}

# ============================================================================
# Chunk-basierte Verarbeitung
# ============================================================================

#' Teilt Daten in optimale Chunks für parallele Verarbeitung
#' 
#' @param n_items Anzahl Elemente
#' @param n_workers Anzahl Worker
#' @param min_chunk_size Minimale Chunk-Größe
#' @return Liste von Index-Vektoren
#' @export
create_chunks <- function(n_items, n_workers = NULL, min_chunk_size = 10) {
  
  if (is.null(n_workers)) {
    n_workers <- get_optimal_workers()
  }
  
  # Optimale Chunk-Größe berechnen
  chunk_size <- max(min_chunk_size, ceiling(n_items / n_workers))
  n_chunks <- ceiling(n_items / chunk_size)
  
  # Indices aufteilen
  chunks <- split(seq_len(n_items), 
                 cut(seq_len(n_items), 
                     breaks = n_chunks, 
                     labels = FALSE))
  
  return(chunks)
}

#' Parallele Verarbeitung mit automatischem Chunking
#' 
#' @param data Zu verarbeitende Daten
#' @param fun Funktion zur Anwendung
#' @param n_cores Anzahl Cores
#' @param chunk_size Chunk-Größe (NULL = auto)
#' @param progress Fortschritt anzeigen
#' @param combine Funktion zum Kombinieren der Ergebnisse
#' @return Kombinierte Ergebnisse
#' @export
parallel_process_chunked <- function(data, 
                                    fun, 
                                    n_cores = NULL,
                                    chunk_size = NULL,
                                    progress = TRUE,
                                    combine = rbind) {
  
  n_cores <- n_cores %||% get_optimal_workers()
  n_items <- if (is.data.frame(data)) nrow(data) else length(data)
  
  # Progress setup
  if (progress && requireNamespace("progressr", quietly = TRUE)) {
    p <- progressr::progressor(steps = n_items)
  } else {
    p <- function(...) invisible(NULL)
  }
  
  # Chunking
  if (is.null(chunk_size)) {
    chunks <- create_chunks(n_items, n_cores)
  } else {
    chunks <- split(seq_len(n_items), 
                   ceiling(seq_len(n_items) / chunk_size))
  }
  
  # Parallel processing
  if (.Platform$OS.type == "unix" && n_cores > 1) {
    # Unix: use mclapply
    results <- parallel::mclapply(chunks, function(idx) {
      if (is.data.frame(data)) {
        chunk_data <- data[idx, , drop = FALSE]
      } else {
        chunk_data <- data[idx]
      }
      result <- fun(chunk_data)
      p(length(idx))
      return(result)
    }, mc.cores = n_cores)
    
  } else {
    # Windows or single-core: use parLapply
    if (n_cores > 1) {
      cl <- make_adaptive_cluster(n_cores)
      on.exit(parallel::stopCluster(cl))
      
      # Export necessary objects
      parallel::clusterExport(cl, c("fun", "data"), envir = environment())
      
      results <- parallel::parLapply(cl, chunks, function(idx) {
        if (is.data.frame(data)) {
          chunk_data <- data[idx, , drop = FALSE]
        } else {
          chunk_data <- data[idx]
        }
        fun(chunk_data)
      })
    } else {
      # Single core fallback
      results <- lapply(chunks, function(idx) {
        if (is.data.frame(data)) {
          chunk_data <- data[idx, , drop = FALSE]
        } else {
          chunk_data <- data[idx]
        }
        result <- fun(chunk_data)
        p(length(idx))
        return(result)
      })
    }
  }
  
  # Combine results
  if (!is.null(combine)) {
    return(do.call(combine, results))
  } else {
    return(results)
  }
}

# ============================================================================
# Load Balancing
# ============================================================================

#' Dynamisches Load Balancing für ungleiche Arbeitslasten
#' 
#' @param tasks Liste von Aufgaben
#' @param worker_fun Worker-Funktion
#' @param n_cores Anzahl Cores
#' @param timeout Timeout in Sekunden
#' @return Liste von Ergebnissen
#' @export
parallel_load_balanced <- function(tasks, 
                                  worker_fun, 
                                  n_cores = NULL,
                                  timeout = Inf) {
  
  n_cores <- n_cores %||% get_optimal_workers()
  n_tasks <- length(tasks)
  
  if (n_cores == 1 || n_tasks <= 1) {
    # Single core: sequential processing
    return(lapply(tasks, worker_fun))
  }
  
  # Create cluster
  cl <- make_adaptive_cluster(n_cores)
  on.exit(parallel::stopCluster(cl))
  
  # Dynamic scheduling with clusterApplyLB
  results <- parallel::clusterApplyLB(cl, tasks, worker_fun)
  
  return(results)
}

# ============================================================================
# Memory-aware Parallelization
# ============================================================================

#' Schätzt Memory-Verbrauch pro Worker
#' 
#' @param sample_data Beispiel-Daten
#' @param fun Zu testende Funktion
#' @return Geschätzter Memory-Verbrauch in MB
#' @export
estimate_memory_per_worker <- function(sample_data, fun) {
  
  # Memory vor Ausführung
  gc(verbose = FALSE)
  mem_before <- sum(gc()[, 2])
  
  # Funktion ausführen
  result <- fun(sample_data)
  
  # Memory nach Ausführung
  gc(verbose = FALSE)
  mem_after <- sum(gc()[, 2])
  
  # Differenz in MB
  mem_used <- max(0, mem_after - mem_before)
  
  # Aufräumen
  rm(result)
  gc(verbose = FALSE)
  
  return(mem_used)
}

#' Memory-aware Anzahl Worker bestimmen
#' 
#' @param total_memory_mb Verfügbarer Speicher in MB
#' @param memory_per_task Memory pro Task in MB
#' @param max_cores Maximale Cores
#' @return Optimale Anzahl Worker
#' @export
get_memory_aware_workers <- function(total_memory_mb = NULL,
                                    memory_per_task = 100,
                                    max_cores = NULL) {
  
  if (is.null(total_memory_mb)) {
    # Versuche verfügbaren RAM zu ermitteln
    if (.Platform$OS.type == "unix") {
      # Unix/Mac
      total_memory_mb <- as.numeric(system("awk '/MemAvailable/ {print $2/1024}' /proc/meminfo", 
                                          intern = TRUE, 
                                          ignore.stderr = TRUE))
      if (length(total_memory_mb) == 0) {
        total_memory_mb <- 4096  # Fallback: 4GB
      }
    } else {
      # Windows: konservativer Default
      total_memory_mb <- 4096
    }
  }
  
  # Workers basierend auf Memory
  memory_workers <- floor(total_memory_mb / memory_per_task)
  
  # CPU-basierte Grenze
  cpu_workers <- get_optimal_workers(max_cores = max_cores)
  
  # Minimum von beiden
  optimal <- min(memory_workers, cpu_workers)
  
  return(max(1, optimal))
}

# ============================================================================
# Shared Memory für große Datensätze
# ============================================================================

#' Setup für Shared Memory (Unix only)
#' 
#' @param data Zu teilende Daten
#' @param name Name des shared memory objects
#' @return Shared memory reference
#' @export
setup_shared_memory <- function(data, name = "tdmx_shared") {
  
  if (.Platform$OS.type != "unix") {
    warning("Shared memory only supported on Unix systems")
    return(NULL)
  }
  
  # Check for bigmemory package
  if (!requireNamespace("bigmemory", quietly = TRUE)) {
    warning("Package 'bigmemory' required for shared memory. Using copies instead.")
    return(NULL)
  }
  
  # Convert to big.matrix for sharing
  if (is.matrix(data) || is.data.frame(data)) {
    shared_data <- bigmemory::as.big.matrix(data, 
                                           type = "double",
                                           shared = TRUE)
    
    # Store descriptor for workers
    desc <- bigmemory::describe(shared_data)
    
    return(list(data = shared_data, desc = desc))
  }
  
  return(NULL)
}

# ============================================================================
# Parallel Utilities Export
# ============================================================================

#' Export-Liste aller Parallel-Utilities
#' @export
parallel_utils <- list(
  get_optimal_workers = get_optimal_workers,
  make_adaptive_cluster = make_adaptive_cluster,
  create_chunks = create_chunks,
  parallel_process_chunked = parallel_process_chunked,
  parallel_load_balanced = parallel_load_balanced,
  estimate_memory_per_worker = estimate_memory_per_worker,
  get_memory_aware_workers = get_memory_aware_workers,
  setup_shared_memory = setup_shared_memory
)

# ============================================================================
# Parallel Configuration Management
# ============================================================================

#' Globale Parallel-Konfiguration
#' @export
tdmx_parallel_config <- new.env(parent = emptyenv())

#' Setzt globale Parallel-Konfiguration
#' @export
configure_parallel <- function(max_cores = NULL,
                              method = "auto",
                              progress = TRUE,
                              memory_limit_mb = NULL) {
  
  tdmx_parallel_config$max_cores <- max_cores
  tdmx_parallel_config$method <- method
  tdmx_parallel_config$progress <- progress
  tdmx_parallel_config$memory_limit_mb <- memory_limit_mb
  
  # Setup future if available
  if (requireNamespace("future", quietly = TRUE)) {
    if (method == "auto") {
      if (.Platform$OS.type == "unix") {
        future::plan(future::multicore, workers = max_cores %||% get_optimal_workers())
      } else {
        future::plan(future::multisession, workers = max_cores %||% get_optimal_workers())
      }
    }
  }
  
  invisible(tdmx_parallel_config)
}

# Initialize with defaults
configure_parallel()

message("Parallel utilities loaded. Detected ", parallel::detectCores(), " cores.")
