# R/job_queue.R
# Job-Queue-System für multiple Fit-Requests mit Prioritäten
# Result-Caching und Retrieval

# Dependencies
if (!requireNamespace("R6", quietly = TRUE)) {
  stop("Package 'R6' required. Install with: install.packages('R6')")
}
if (!requireNamespace("digest", quietly = TRUE)) {
  warning("Package 'digest' recommended for caching. Install with: install.packages('digest')")
}
if (!requireNamespace("future", quietly = TRUE)) {
  warning("Package 'future' recommended. Install with: install.packages('future')")
}

# ============================================================================
# Job Queue Implementation
# ============================================================================

#' R6 Class für Job Queue Management
#' @export
JobQueue <- R6::R6Class(
  "JobQueue",
  
  private = list(
    .queue = list(),
    .running = list(),
    .completed = list(),
    .failed = list(),
    .max_workers = NULL,
    .cache_dir = NULL,
    .result_cache = new.env(parent = emptyenv()),
    .job_counter = 0,
    .worker_pool = NULL,
    .monitoring = FALSE,
    .stats = list(),
    
    # Generate unique job ID
    generate_job_id = function() {
      private$.job_counter <- private$.job_counter + 1
      sprintf("job_%s_%06d", 
              format(Sys.time(), "%Y%m%d_%H%M%S"),
              private$.job_counter)
    },
    
    # Calculate job hash for caching
    calculate_job_hash = function(job_spec) {
      if (requireNamespace("digest", quietly = TRUE)) {
        digest::digest(job_spec, algo = "xxhash64")
      } else {
        # Fallback: simple string concatenation
        paste(job_spec$type, job_spec$priority, collapse = "_")
      }
    },
    
    # Check cache for existing result
    check_cache = function(job_hash) {
      # Memory cache
      if (exists(job_hash, envir = private$.result_cache)) {
        return(get(job_hash, envir = private$.result_cache))
      }
      
      # Disk cache
      if (!is.null(private$.cache_dir)) {
        cache_file <- file.path(private$.cache_dir, paste0(job_hash, ".rds"))
        if (file.exists(cache_file)) {
          # Check age
          file_age <- difftime(Sys.time(), file.info(cache_file)$mtime, units = "hours")
          max_age <- getOption("tdmx_cache_max_age_hours", 24)
          
          if (file_age < max_age) {
            result <- readRDS(cache_file)
            # Store in memory cache
            assign(job_hash, result, envir = private$.result_cache)
            return(result)
          }
        }
      }
      
      return(NULL)
    },
    
    # Save result to cache
    save_to_cache = function(job_hash, result) {
      # Memory cache
      assign(job_hash, result, envir = private$.result_cache)
      
      # Disk cache
      if (!is.null(private$.cache_dir)) {
        if (!dir.exists(private$.cache_dir)) {
          dir.create(private$.cache_dir, recursive = TRUE)
        }
        cache_file <- file.path(private$.cache_dir, paste0(job_hash, ".rds"))
        saveRDS(result, cache_file)
      }
    },
    
    # Process next job in queue
    process_next = function() {
      if (length(private$.queue) == 0) {
        return(NULL)
      }
      
      # Check worker availability
      n_running <- length(private$.running)
      if (n_running >= private$.max_workers) {
        return(NULL)
      }
      
      # Get highest priority job
      priorities <- sapply(private$.queue, function(j) j$priority)
      next_idx <- which.max(priorities)
      job <- private$.queue[[next_idx]]
      private$.queue[[next_idx]] <- NULL
      
      # Move to running
      private$.running[[job$id]] <- job
      job$status <- "running"
      job$start_time <- Sys.time()
      
      # Execute job
      private$execute_job(job)
      
      return(job$id)
    },
    
    # Execute a single job
    execute_job = function(job) {
      
      # Check cache first
      job_hash <- private$calculate_job_hash(job$spec)
      cached_result <- private$check_cache(job_hash)
      
      if (!is.null(cached_result)) {
        # Use cached result
        job$result <- cached_result
        job$status <- "completed"
        job$end_time <- Sys.time()
        job$from_cache <- TRUE
        
        # Move to completed
        private$.running[[job$id]] <- NULL
        private$.completed[[job$id]] <- job
        
        # Trigger callback
        if (!is.null(job$callback)) {
          job$callback(job$result)
        }
        
        return(invisible(job))
      }
      
      # Execute job asynchronously
      if (requireNamespace("future", quietly = TRUE)) {
        job$future <- future::future({
          # Execute based on job type
          result <- switch(job$type,
            "stan_fit" = private$execute_stan_fit(job$spec),
            "pta_calculation" = private$execute_pta_calc(job$spec),
            "optimization" = private$execute_optimization(job$spec),
            "batch_processing" = private$execute_batch(job$spec),
            stop("Unknown job type: ", job$type)
          )
          return(result)
        }, lazy = FALSE)
        
        # Setup completion handler
        promises::then(
          promises::future_promise(job$future),
          onFulfilled = function(result) {
            job$result <- result
            job$status <- "completed"
            job$end_time <- Sys.time()
            job$from_cache <- FALSE
            
            # Save to cache
            private$save_to_cache(job_hash, result)
            
            # Move to completed
            private$.running[[job$id]] <- NULL
            private$.completed[[job$id]] <- job
            
            # Trigger callback
            if (!is.null(job$callback)) {
              job$callback(result)
            }
            
            # Process next job
            private$process_next()
          },
          onRejected = function(error) {
            job$error <- error
            job$status <- "failed"
            job$end_time <- Sys.time()
            
            # Move to failed
            private$.running[[job$id]] <- NULL
            private$.failed[[job$id]] <- job
            
            # Trigger error callback
            if (!is.null(job$error_callback)) {
              job$error_callback(error)
            }
            
            # Process next job
            private$process_next()
          }
        )
      } else {
        # Synchronous fallback
        tryCatch({
          result <- switch(job$type,
            "stan_fit" = private$execute_stan_fit(job$spec),
            "pta_calculation" = private$execute_pta_calc(job$spec),
            "optimization" = private$execute_optimization(job$spec),
            "batch_processing" = private$execute_batch(job$spec),
            stop("Unknown job type: ", job$type)
          )
          
          job$result <- result
          job$status <- "completed"
          job$end_time <- Sys.time()
          job$from_cache <- FALSE
          
          # Save to cache
          private$save_to_cache(job_hash, result)
          
          # Move to completed
          private$.running[[job$id]] <- NULL
          private$.completed[[job$id]] <- job
          
          # Trigger callback
          if (!is.null(job$callback)) {
            job$callback(result)
          }
          
        }, error = function(e) {
          job$error <- e
          job$status <- "failed"
          job$end_time <- Sys.time()
          
          # Move to failed
          private$.running[[job$id]] <- NULL
          private$.failed[[job$id]] <- job
          
          # Trigger error callback
          if (!is.null(job$error_callback)) {
            job$error_callback(e)
          }
        })
        
        # Process next job
        private$process_next()
      }
    },
    
    # Execute Stan fit job
    execute_stan_fit = function(spec) {
      if (file.exists("R/backend_bayes.R")) {
        source("R/backend_bayes.R")
      }
      
      # Use existing run_fit_stan_hmc or similar
      result <- do.call(run_fit_stan_hmc, spec)
      return(result)
    },
    
    # Execute PTA calculation job
    execute_pta_calc = function(spec) {
      if (file.exists("R/pta_cfr.R")) {
        source("R/pta_cfr.R")
      }
      
      # Parallel PTA if available
      if (exists("pta_parallel")) {
        result <- pta_parallel(
          draws = spec$draws,
          regimen = spec$regimen,
          n_cores = spec$n_cores %||% get_optimal_workers()
        )
      } else {
        # Fallback to standard PTA
        result <- pta_for_regimen(
          draws = spec$draws,
          regimen = spec$regimen,
          model_type = spec$model_type,
          target_def = spec$target_def,
          MIC = spec$MIC
        )
      }
      
      return(result)
    },
    
    # Execute optimization job
    execute_optimization = function(spec) {
      # Placeholder for optimization logic
      message("Executing optimization job...")
      Sys.sleep(2)  # Simulate work
      return(list(optimal_dose = spec$dose * 1.1))
    },
    
    # Execute batch processing job
    execute_batch = function(spec) {
      if (exists("parallel_process_chunked")) {
        result <- parallel_process_chunked(
          data = spec$data,
          fun = spec$fun,
          n_cores = spec$n_cores %||% get_optimal_workers()
        )
      } else {
        # Fallback to lapply
        result <- lapply(spec$data, spec$fun)
      }
      
      return(result)
    },
    
    # Monitor queue statistics
    monitor_stats = function() {
      stats <- list(
        timestamp = Sys.time(),
        queued = length(private$.queue),
        running = length(private$.running),
        completed = length(private$.completed),
        failed = length(private$.failed),
        cache_hits = sum(sapply(private$.completed, function(j) j$from_cache %||% FALSE)),
        avg_wait_time = mean(sapply(private$.completed, function(j) {
          if (!is.null(j$start_time) && !is.null(j$submit_time)) {
            as.numeric(difftime(j$start_time, j$submit_time, units = "secs"))
          } else {
            NA
          }
        }), na.rm = TRUE),
        avg_run_time = mean(sapply(private$.completed, function(j) {
          if (!is.null(j$end_time) && !is.null(j$start_time)) {
            as.numeric(difftime(j$end_time, j$start_time, units = "secs"))
          } else {
            NA
          }
        }), na.rm = TRUE)
      )
      
      private$.stats[[length(private$.stats) + 1]] <- stats
      return(stats)
    }
  ),
  
  public = list(
    
    #' Initialize Job Queue
    #' @param max_workers Maximum concurrent workers
    #' @param cache_dir Directory for result caching
    initialize = function(max_workers = NULL, cache_dir = NULL) {
      if (is.null(max_workers)) {
        max_workers <- get_optimal_workers(task_type = "memory")
      }
      
      private$.max_workers <- max_workers
      private$.cache_dir <- cache_dir
      
      # Setup cache directory
      if (!is.null(cache_dir) && !dir.exists(cache_dir)) {
        dir.create(cache_dir, recursive = TRUE)
      }
      
      message(sprintf("Job queue initialized with %d workers", max_workers))
    },
    
    #' Submit job to queue
    #' @param type Job type
    #' @param spec Job specification
    #' @param priority Priority (higher = sooner)
    #' @param callback Success callback
    #' @param error_callback Error callback
    #' @return Job ID
    submit = function(type, 
                     spec, 
                     priority = 5,
                     callback = NULL,
                     error_callback = NULL) {
      
      job_id <- private$generate_job_id()
      
      job <- list(
        id = job_id,
        type = type,
        spec = spec,
        priority = priority,
        status = "queued",
        submit_time = Sys.time(),
        start_time = NULL,
        end_time = NULL,
        result = NULL,
        error = NULL,
        callback = callback,
        error_callback = error_callback,
        from_cache = FALSE
      )
      
      # Add to queue
      private$.queue[[length(private$.queue) + 1]] <- job
      
      # Try to process immediately
      private$process_next()
      
      return(job_id)
    },
    
    #' Submit batch of jobs
    #' @param jobs List of job specifications
    #' @return Vector of job IDs
    submit_batch = function(jobs) {
      job_ids <- character(length(jobs))
      
      for (i in seq_along(jobs)) {
        job_ids[i] <- self$submit(
          type = jobs[[i]]$type,
          spec = jobs[[i]]$spec,
          priority = jobs[[i]]$priority %||% 5,
          callback = jobs[[i]]$callback,
          error_callback = jobs[[i]]$error_callback
        )
      }
      
      return(job_ids)
    },
    
    #' Get job status
    #' @param job_id Job ID
    #' @return Job status
    status = function(job_id) {
      # Check in order: running, completed, failed, queued
      if (job_id %in% names(private$.running)) {
        return(private$.running[[job_id]]$status)
      }
      if (job_id %in% names(private$.completed)) {
        return("completed")
      }
      if (job_id %in% names(private$.failed)) {
        return("failed")
      }
      
      # Check queue
      for (job in private$.queue) {
        if (job$id == job_id) {
          return("queued")
        }
      }
      
      return("not_found")
    },
    
    #' Get job result
    #' @param job_id Job ID
    #' @param wait Wait for completion
    #' @param timeout Timeout in seconds
    #' @return Job result or NULL
    result = function(job_id, wait = FALSE, timeout = 60) {
      
      if (wait) {
        start_time <- Sys.time()
        while (difftime(Sys.time(), start_time, units = "secs") < timeout) {
          status <- self$status(job_id)
          
          if (status == "completed") {
            return(private$.completed[[job_id]]$result)
          } else if (status == "failed") {
            stop("Job failed: ", private$.failed[[job_id]]$error$message)
          } else if (status == "not_found") {
            stop("Job not found: ", job_id)
          }
          
          Sys.sleep(0.5)
        }
        
        stop("Timeout waiting for job: ", job_id)
      } else {
        if (job_id %in% names(private$.completed)) {
          return(private$.completed[[job_id]]$result)
        } else {
          return(NULL)
        }
      }
    },
    
    #' Cancel job
    #' @param job_id Job ID
    #' @return Success indicator
    cancel = function(job_id) {
      # Remove from queue
      for (i in seq_along(private$.queue)) {
        if (private$.queue[[i]]$id == job_id) {
          private$.queue[[i]] <- NULL
          return(TRUE)
        }
      }
      
      # Cancel if running (if using futures)
      if (job_id %in% names(private$.running)) {
        job <- private$.running[[job_id]]
        if (!is.null(job$future) && requireNamespace("future", quietly = TRUE)) {
          # Note: future cancellation is limited
          warning("Cannot cancel running job in R")
        }
      }
      
      return(FALSE)
    },
    
    #' Clear completed/failed jobs
    #' @param type "completed", "failed", or "all"
    clear = function(type = "completed") {
      if (type %in% c("completed", "all")) {
        private$.completed <- list()
      }
      if (type %in% c("failed", "all")) {
        private$.failed <- list()
      }
      
      invisible(self)
    },
    
    #' Get queue statistics
    #' @return Statistics list
    get_stats = function() {
      private$monitor_stats()
    },
    
    #' Get all statistics history
    #' @return List of statistics
    get_stats_history = function() {
      private$.stats
    },
    
    #' Print queue status
    print = function() {
      cat("TDMx Job Queue\n")
      cat("--------------\n")
      cat("Max workers:  ", private$.max_workers, "\n")
      cat("Queued:       ", length(private$.queue), "\n")
      cat("Running:      ", length(private$.running), "\n")
      cat("Completed:    ", length(private$.completed), "\n")
      cat("Failed:       ", length(private$.failed), "\n")
      cat("Cache size:   ", length(ls(private$.result_cache)), " results\n")
      
      if (length(private$.queue) > 0) {
        cat("\nQueued jobs:\n")
        for (job in private$.queue) {
          cat(sprintf("  %s (priority: %d, type: %s)\n", 
                     job$id, job$priority, job$type))
        }
      }
      
      if (length(private$.running) > 0) {
        cat("\nRunning jobs:\n")
        for (job in private$.running) {
          runtime <- difftime(Sys.time(), job$start_time, units = "secs")
          cat(sprintf("  %s (runtime: %.1fs, type: %s)\n", 
                     job$id, runtime, job$type))
        }
      }
      
      invisible(self)
    },
    
    #' Clear cache
    #' @param memory Clear memory cache
    #' @param disk Clear disk cache
    clear_cache = function(memory = TRUE, disk = FALSE) {
      if (memory) {
        rm(list = ls(private$.result_cache), envir = private$.result_cache)
        gc(verbose = FALSE)
      }
      
      if (disk && !is.null(private$.cache_dir)) {
        cache_files <- list.files(private$.cache_dir, pattern = "\\.rds$", 
                                 full.names = TRUE)
        file.remove(cache_files)
      }
      
      invisible(self)
    },
    
    #' Wait for all jobs to complete
    #' @param timeout Maximum wait time in seconds
    wait_all = function(timeout = 600) {
      start_time <- Sys.time()
      
      while (length(private$.queue) > 0 || length(private$.running) > 0) {
        if (difftime(Sys.time(), start_time, units = "secs") > timeout) {
          warning("Timeout waiting for all jobs")
          break
        }
        Sys.sleep(0.5)
      }
      
      invisible(self)
    }
  )
)

# ============================================================================
# Global Job Queue Instance
# ============================================================================

#' Get or create global job queue
#' @param max_workers Maximum workers
#' @param cache_dir Cache directory
#' @return JobQueue instance
#' @export
get_job_queue <- function(max_workers = NULL, cache_dir = NULL) {
  
  if (!exists(".tdmx_job_queue", envir = .GlobalEnv)) {
    .tdmx_job_queue <<- JobQueue$new(
      max_workers = max_workers,
      cache_dir = cache_dir %||% file.path(tempdir(), "tdmx_job_cache")
    )
  }
  
  return(.tdmx_job_queue)
}

# ============================================================================
# Convenience Functions
# ============================================================================

#' Submit Stan fit job
#' @param ... Parameters for Stan fit
#' @param priority Job priority
#' @param callback Completion callback
#' @return Job ID
#' @export
submit_stan_job <- function(..., priority = 5, callback = NULL) {
  queue <- get_job_queue()
  
  queue$submit(
    type = "stan_fit",
    spec = list(...),
    priority = priority,
    callback = callback
  )
}

#' Submit PTA calculation job
#' @param ... Parameters for PTA calculation
#' @param priority Job priority
#' @param callback Completion callback
#' @return Job ID
#' @export
submit_pta_job <- function(..., priority = 5, callback = NULL) {
  queue <- get_job_queue()
  
  queue$submit(
    type = "pta_calculation",
    spec = list(...),
    priority = priority,
    callback = callback
  )
}

#' Submit batch processing job
#' @param data Data to process
#' @param fun Function to apply
#' @param priority Job priority
#' @param callback Completion callback
#' @return Job ID
#' @export
submit_batch_job <- function(data, fun, priority = 3, callback = NULL) {
  queue <- get_job_queue()
  
  queue$submit(
    type = "batch_processing",
    spec = list(data = data, fun = fun),
    priority = priority,
    callback = callback
  )
}

# ============================================================================
# Job Queue Monitor (for Shiny)
# ============================================================================

#' Create Shiny-compatible queue monitor
#' @param queue JobQueue instance
#' @param session Shiny session
#' @param update_interval Update interval in seconds
#' @return Monitor object
#' @export
create_queue_monitor <- function(queue = NULL, session = NULL, update_interval = 2) {
  
  if (is.null(queue)) {
    queue <- get_job_queue()
  }
  
  monitoring <- TRUE
  
  monitor <- function() {
    while (monitoring) {
      stats <- queue$get_stats()
      
      if (!is.null(session)) {
        session$sendCustomMessage("updateQueueStats", stats)
      }
      
      Sys.sleep(update_interval)
    }
  }
  
  # Start monitoring
  if (requireNamespace("future", quietly = TRUE)) {
    future::future(monitor(), lazy = FALSE)
  }
  
  list(
    stop = function() { monitoring <<- FALSE },
    queue = queue
  )
}

# Initialize message
message("Job queue system loaded. Use get_job_queue() to access.")
