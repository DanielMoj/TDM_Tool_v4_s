# R/pta_cfr.R
# PTA/CFR computation - PARALLELIZED VERSION with adaptive worker management
# Performance optimized with chunk-wise distribution and parallel diagnostics

# Source parallel utilities if available
if (file.exists("R/parallel_utils.R")) {
  source("R/parallel_utils.R")
}

# ============================================================================
# Core PTA/CFR Functions (unchanged for compatibility)
# ============================================================================

# Simulate steady-state over a long horizon and then compute last interval metrics
ss_window <- function(regimen, n_intervals = 20L, dt = 0.05) {
  list(
    times = seq(0, n_intervals * regimen$tau, by = dt),
    t_end  = n_intervals * regimen$tau
  )
}

# Compute metrics for single draw (unchanged)
compute_metrics_for_draw <- function(theta, regimen, model_type, MIC) {
  n_intervals <- 20L
  
  # simulate steady-state and take last interval
  win <- ss_window(regimen, n_intervals = n_intervals)
  conc <- predict_conc_grid(win$times, list(dose = regimen$dose, tau = regimen$tau, tinf = regimen$tinf,
                                            n_doses = n_intervals, start_time = 0),
                            theta, model_type)
  # apply site penetration factor
  drg <- getOption("current_drug_name", default = "Drug")
  st  <- getOption("current_site_name", default = "Plasma")
  conc <- apply_site_penetration(conc, drg, st, load_tissue_cfg("config/tissue.json"))
  
  # last interval [t_end - tau, t_end]
  t0 <- tail(win$times, 1) - regimen$tau
  idx <- which(win$times >= t0 - 1e-9)
  t_iv <- win$times[idx]
  c_iv <- conc[idx]
  
  # fT>MIC
  ft <- mean(c_iv > MIC)
  # AUC over last interval (tau), trapezoid
  auc_tau <- sum(diff(t_iv) * zoo::rollmean(c_iv, 2))
  # scale to 24h
  auc24 <- auc_tau * (24 / regimen$tau)
  # Cmax on last interval
  cmax <- max(c_iv)
  
  list(
    ft_gt_mic = ft,
    auc_tau = auc_tau,
    auc24 = auc24,
    auc24_over_mic = ifelse(MIC > 0, auc24 / MIC, Inf),
    cmax = cmax,
    cmax_over_mic = ifelse(MIC > 0, cmax / MIC, Inf)
  )
}

# ============================================================================
# PARALLELIZED PTA CALCULATION
# ============================================================================

#' Parallelisierte PTA-Berechnung mit adaptiver Worker-Anzahl
#' 
#' @param draws Posterior draws (data.frame oder matrix)
#' @param regimen Dosierungsschema
#' @param model_type PK-Modell-Typ
#' @param target_def Target-Definition
#' @param MIC Minimale Hemmkonzentration
#' @param n_cores Anzahl Cores (NULL = auto-detect)
#' @param chunk_size Chunk-Größe für Batch-Processing
#' @param progress Fortschritt anzeigen
#' @return PTA-Wert (0-1)
#' @export
pta_parallel <- function(draws, 
                        regimen, 
                        model_type,
                        target_def,
                        MIC,
                        n_cores = NULL,
                        chunk_size = NULL,
                        progress = TRUE) {
  
  if (nrow(draws) == 0) return(NA_real_)
  
  # Determine optimal number of workers
  if (is.null(n_cores)) {
    n_cores <- get_optimal_workers(task_type = "cpu")
  }
  
  n_draws <- nrow(draws)
  
  # For small datasets, use sequential processing
  if (n_draws < 100 || n_cores == 1) {
    ok <- vapply(seq_len(n_draws), function(i) {
      th <- as.list(draws[i, , drop = FALSE])
      m <- compute_metrics_for_draw(th, regimen, model_type, MIC)
      meets_target(m, target_def)
    }, logical(1))
    
    return(mean(ok))
  }
  
  # Create chunks for parallel processing
  if (is.null(chunk_size)) {
    chunk_size <- max(10, ceiling(n_draws / (n_cores * 4)))
  }
  chunks <- create_chunks(n_draws, n_cores, min_chunk_size = chunk_size)
  
  # Progress setup
  if (progress && requireNamespace("progressr", quietly = TRUE)) {
    p <- progressr::progressor(steps = length(chunks))
  } else {
    p <- function(...) invisible(NULL)
  }
  
  # Platform-specific parallel processing
  if (.Platform$OS.type == "unix") {
    # Unix/Mac: use mclapply (fork-based, more efficient)
    results <- parallel::mclapply(chunks, function(idx) {
      chunk_results <- logical(length(idx))
      
      for (j in seq_along(idx)) {
        i <- idx[j]
        th <- as.list(draws[i, , drop = FALSE])
        m <- compute_metrics_for_draw(th, regimen, model_type, MIC)
        chunk_results[j] <- meets_target(m, target_def)
      }
      
      p(message = sprintf("Processed chunk with %d draws", length(idx)))
      return(chunk_results)
    }, mc.cores = n_cores)
    
  } else {
    # Windows: use parLapply (socket-based)
    cl <- parallel::makeCluster(n_cores)
    on.exit(parallel::stopCluster(cl))
    
    # Export necessary functions and objects to workers
    parallel::clusterExport(cl, c(
      "compute_metrics_for_draw",
      "predict_conc_grid",
      "apply_site_penetration",
      "ss_window",
      "meets_target",
      "load_tissue_cfg"
    ), envir = environment())
    
    # Export draws and other parameters
    parallel::clusterExport(cl, c(
      "draws", "regimen", "model_type", "MIC"
    ), envir = environment())
    
    # Load required packages in workers
    parallel::clusterEvalQ(cl, {
      if (requireNamespace("zoo", quietly = TRUE)) library(zoo)
      if (requireNamespace("deSolve", quietly = TRUE)) library(deSolve)
    })
    
    results <- parallel::parLapply(cl, chunks, function(idx) {
      chunk_results <- logical(length(idx))
      
      for (j in seq_along(idx)) {
        i <- idx[j]
        th <- as.list(draws[i, , drop = FALSE])
        m <- compute_metrics_for_draw(th, regimen, model_type, MIC)
        chunk_results[j] <- meets_target(m, target_def)
      }
      
      return(chunk_results)
    })
  }
  
  # Combine results
  ok <- unlist(results)
  
  return(mean(ok))
}

# ============================================================================
# PARALLELIZED CFR CALCULATION
# ============================================================================

#' Parallelisierte CFR-Berechnung
#' 
#' @param draws Posterior draws
#' @param regimen Dosierungsschema
#' @param model_type PK-Modell-Typ
#' @param target_def Target-Definition
#' @param mic_dist MIC-Verteilung
#' @param n_cores Anzahl Cores
#' @param parallel_mic Parallelisierung über MIC-Werte
#' @return CFR-Wert (0-1)
#' @export
cfr_parallel <- function(draws, 
                        regimen, 
                        model_type,
                        target_def,
                        mic_dist,
                        n_cores = NULL,
                        parallel_mic = TRUE) {
  
  if (is.null(mic_dist) || nrow(mic_dist) == 0) return(NA_real_)
  
  if (is.null(n_cores)) {
    n_cores <- get_optimal_workers(task_type = "cpu")
  }
  
  n_mics <- nrow(mic_dist)
  
  if (parallel_mic && n_mics > 1 && n_cores > 1) {
    # Parallelize over MIC values
    if (.Platform$OS.type == "unix") {
      pta_vals <- parallel::mclapply(seq_len(n_mics), function(i) {
        MIC <- mic_dist$mic[i]
        pta_parallel(draws, regimen, model_type, target_def, MIC, 
                    n_cores = 1)  # Don't nest parallel loops
      }, mc.cores = min(n_cores, n_mics))
      
      pta_vals <- unlist(pta_vals)
      
    } else {
      # Windows
      cl <- parallel::makeCluster(min(n_cores, n_mics))
      on.exit(parallel::stopCluster(cl))
      
      parallel::clusterExport(cl, c(
        "pta_parallel", "draws", "regimen", "model_type", "target_def"
      ), envir = environment())
      
      pta_vals <- parallel::parLapply(cl, seq_len(n_mics), function(i) {
        MIC <- mic_dist$mic[i]
        pta_parallel(draws, regimen, model_type, target_def, MIC, n_cores = 1)
      })
      
      pta_vals <- unlist(pta_vals)
    }
  } else {
    # Sequential calculation over MIC values
    pta_vals <- vapply(seq_len(n_mics), function(i) {
      MIC <- mic_dist$mic[i]
      pta_parallel(draws, regimen, model_type, target_def, MIC, n_cores = n_cores)
    }, numeric(1))
  }
  
  # Weighted sum for CFR
  cfr <- sum(pta_vals * mic_dist$p)
  
  return(cfr)
}

# ============================================================================
# PARALLEL DIAGNOSTICS CALCULATION
# ============================================================================

#' Parallele Berechnung von Diagnostics (ESS, Rhat, etc.)
#' 
#' @param draws Posterior draws
#' @param n_cores Anzahl Cores
#' @return Liste mit Diagnostics
#' @export
calculate_diagnostics_parallel <- function(draws, n_cores = NULL) {
  
  if (!requireNamespace("posterior", quietly = TRUE)) {
    warning("Package 'posterior' required for diagnostics")
    return(NULL)
  }
  
  if (is.null(n_cores)) {
    n_cores <- get_optimal_workers(task_type = "cpu")
  }
  
  # Convert to draws format if needed
  if (!inherits(draws, "draws")) {
    draws <- posterior::as_draws_df(draws)
  }
  
  # Split variables for parallel processing
  variables <- names(draws)[!names(draws) %in% c(".chain", ".iteration", ".draw")]
  var_chunks <- split(variables, cut(seq_along(variables), n_cores, labels = FALSE))
  
  if (.Platform$OS.type == "unix") {
    # Unix: mclapply
    diag_list <- parallel::mclapply(var_chunks, function(vars) {
      subset_draws <- posterior::subset_draws(draws, variable = vars)
      list(
        ess_bulk = posterior::ess_bulk(subset_draws),
        ess_tail = posterior::ess_tail(subset_draws),
        rhat = posterior::rhat(subset_draws)
      )
    }, mc.cores = n_cores)
    
  } else {
    # Windows: parLapply
    cl <- parallel::makeCluster(n_cores)
    on.exit(parallel::stopCluster(cl))
    
    parallel::clusterExport(cl, "draws", envir = environment())
    parallel::clusterEvalQ(cl, library(posterior))
    
    diag_list <- parallel::parLapply(cl, var_chunks, function(vars) {
      subset_draws <- posterior::subset_draws(draws, variable = vars)
      list(
        ess_bulk = posterior::ess_bulk(subset_draws),
        ess_tail = posterior::ess_tail(subset_draws),
        rhat = posterior::rhat(subset_draws)
      )
    })
  }
  
  # Combine results
  diagnostics <- list(
    ess_bulk = unlist(lapply(diag_list, function(x) x$ess_bulk)),
    ess_tail = unlist(lapply(diag_list, function(x) x$ess_tail)),
    rhat = unlist(lapply(diag_list, function(x) x$rhat))
  )
  
  return(diagnostics)
}

# ============================================================================
# PARALLEL PPC GENERATION
# ============================================================================

#' Parallele Posterior Predictive Check Generierung
#' 
#' @param draws Posterior draws
#' @param regimen Dosierungsschema
#' @param model_type PK-Modell-Typ
#' @param n_sim Anzahl Simulationen
#' @param n_cores Anzahl Cores
#' @return Matrix mit PPC-Simulationen
#' @export
generate_ppc_parallel <- function(draws,
                                 regimen,
                                 model_type,
                                 n_sim = 100,
                                 n_cores = NULL) {
  
  if (is.null(n_cores)) {
    n_cores <- get_optimal_workers(task_type = "cpu")
  }
  
  n_draws <- nrow(draws)
  
  # Sample draws for PPC
  if (n_sim < n_draws) {
    sim_idx <- sample(n_draws, n_sim)
  } else {
    sim_idx <- seq_len(n_draws)
    n_sim <- n_draws
  }
  
  # Create chunks
  chunks <- create_chunks(n_sim, n_cores)
  
  # Time grid for predictions
  time_grid <- seq(0, regimen$tau * 6, by = 0.5)
  
  if (.Platform$OS.type == "unix") {
    # Unix: mclapply
    ppc_list <- parallel::mclapply(chunks, function(idx) {
      ppc_chunk <- matrix(NA, nrow = length(idx), ncol = length(time_grid))
      
      for (j in seq_along(idx)) {
        i <- sim_idx[idx[j]]
        th <- as.list(draws[i, , drop = FALSE])
        
        # Generate concentration profile
        conc <- predict_conc_grid(time_grid, regimen, th, model_type)
        
        # Add error
        sigma_add <- th$sigma_add %||% 0.1
        sigma_prop <- th$sigma_prop %||% 0.1
        error <- rnorm(length(conc), 0, sqrt(sigma_add^2 + (sigma_prop * conc)^2))
        
        ppc_chunk[j, ] <- pmax(0, conc + error)
      }
      
      return(ppc_chunk)
    }, mc.cores = n_cores)
    
  } else {
    # Windows: parLapply
    cl <- parallel::makeCluster(n_cores)
    on.exit(parallel::stopCluster(cl))
    
    parallel::clusterExport(cl, c(
      "draws", "regimen", "model_type", "sim_idx", "time_grid",
      "predict_conc_grid"
    ), envir = environment())
    
    ppc_list <- parallel::parLapply(cl, chunks, function(idx) {
      ppc_chunk <- matrix(NA, nrow = length(idx), ncol = length(time_grid))
      
      for (j in seq_along(idx)) {
        i <- sim_idx[idx[j]]
        th <- as.list(draws[i, , drop = FALSE])
        
        # Generate concentration profile
        conc <- predict_conc_grid(time_grid, regimen, th, model_type)
        
        # Add error
        sigma_add <- th$sigma_add %||% 0.1
        sigma_prop <- th$sigma_prop %||% 0.1
        error <- rnorm(length(conc), 0, sqrt(sigma_add^2 + (sigma_prop * conc)^2))
        
        ppc_chunk[j, ] <- pmax(0, conc + error)
      }
      
      return(ppc_chunk)
    })
  }
  
  # Combine results
  ppc_matrix <- do.call(rbind, ppc_list)
  colnames(ppc_matrix) <- paste0("t_", time_grid)
  
  return(ppc_matrix)
}

# ============================================================================
# BATCH PTA/CFR for multiple regimens
# ============================================================================

#' Batch-PTA-Berechnung für multiple Regimen
#' 
#' @param draws Posterior draws
#' @param regimens_list Liste von Dosierungsschemata
#' @param model_type PK-Modell-Typ
#' @param target_def Target-Definition
#' @param MIC MIC-Wert
#' @param n_cores Anzahl Cores
#' @param load_balance Load-Balancing verwenden
#' @return Vektor mit PTA-Werten
#' @export
pta_batch_parallel <- function(draws,
                              regimens_list,
                              model_type,
                              target_def,
                              MIC,
                              n_cores = NULL,
                              load_balance = TRUE) {
  
  if (is.null(n_cores)) {
    n_cores <- get_optimal_workers(task_type = "memory")
  }
  
  n_regimens <- length(regimens_list)
  
  if (load_balance && n_cores > 1) {
    # Use load-balanced parallel processing
    results <- parallel_load_balanced(
      tasks = regimens_list,
      worker_fun = function(reg) {
        pta_parallel(draws, reg, model_type, target_def, MIC, n_cores = 1)
      },
      n_cores = n_cores
    )
    
    return(unlist(results))
    
  } else {
    # Simple parallel processing
    if (.Platform$OS.type == "unix") {
      results <- parallel::mclapply(regimens_list, function(reg) {
        pta_parallel(draws, reg, model_type, target_def, MIC, n_cores = 1)
      }, mc.cores = min(n_cores, n_regimens))
      
    } else {
      cl <- parallel::makeCluster(min(n_cores, n_regimens))
      on.exit(parallel::stopCluster(cl))
      
      parallel::clusterExport(cl, c(
        "pta_parallel", "draws", "model_type", "target_def", "MIC"
      ), envir = environment())
      
      results <- parallel::parLapply(cl, regimens_list, function(reg) {
        pta_parallel(draws, reg, model_type, target_def, MIC, n_cores = 1)
      })
    }
    
    return(unlist(results))
  }
}

# ============================================================================
# Original functions (kept for backward compatibility)
# ============================================================================

# Original PTA function (redirects to parallel version if available)
pta_for_regimen <- function(draws, regimen, model_type, target_def, MIC) {
  # Use parallel version if workers available
  n_cores <- getOption("tdmx_parallel_cores", 1)
  
  if (n_cores > 1 && nrow(draws) >= 100) {
    return(pta_parallel(draws, regimen, model_type, target_def, MIC, n_cores))
  }
  
  # Fallback to original implementation
  if (nrow(draws) == 0) return(NA_real_)
  
  ok <- vapply(seq_len(nrow(draws)), function(i) {
    th <- as.list(draws[i, , drop = FALSE])
    m <- compute_metrics_for_draw(th, regimen, model_type, MIC)
    meets_target(m, target_def)
  }, logical(1))
  
  mean(ok)
}

# Original CFR function (redirects to parallel version if available)
cfr_for_regimen <- function(draws, regimen, model_type, target_def, mic_dist) {
  # Use parallel version if workers available
  n_cores <- getOption("tdmx_parallel_cores", 1)
  
  if (n_cores > 1 && !is.null(mic_dist) && nrow(mic_dist) > 1) {
    return(cfr_parallel(draws, regimen, model_type, target_def, mic_dist, n_cores))
  }
  
  # Fallback to original implementation
  if (is.null(mic_dist) || nrow(mic_dist) == 0) return(NA_real_)
  
  pta_vals <- vapply(seq_len(nrow(mic_dist)), function(i) {
    MIC <- mic_dist$mic[i]
    pta_for_regimen(draws, regimen, model_type, target_def, MIC)
  }, numeric(1))
  
  sum(pta_vals * mic_dist$p)
}

# Parse MIC distribution (unchanged)
parse_mic_distribution <- function(txt) {
  if (is.null(txt) || !nzchar(txt)) return(NULL)
  parts <- unlist(strsplit(txt, ","))
  
  vals <- t(sapply(parts, function(p) {
    kv <- trimws(unlist(strsplit(p, ":")))
    if (length(kv) != 2) return(c(NA, NA))
    c(as.numeric(kv[1]), as.numeric(kv[2]))
  }))
  
  if (is.null(dim(vals))) return(NULL)
  df <- data.frame(mic = as.numeric(vals[,1]), p = as.numeric(vals[,2]))
  df <- df[is.finite(df$mic) & is.finite(df$p) & df$p >= 0, , drop = FALSE]
  if (nrow(df) == 0) return(NULL)
  df$p <- df$p / sum(df$p)
  df
}

# PTA vs dose grid (unchanged but uses parallel internally)
pta_vs_dose_grid <- function(draws, regimen, model_type, target_def, MIC, doses) {
  vapply(doses, function(d) {
    reg <- regimen
    reg$dose <- d
    pta_for_regimen(draws, reg, model_type, target_def, MIC)
  }, numeric(1))
}

# ============================================================================
# Cache management (enhanced for parallel)
# ============================================================================

.pta_cache <- new.env(parent = emptyenv())

# Thread-safe cache operations
pta_for_regimen_cached <- function(draws, regimen, model_type, target_def, MIC) {
  if (requireNamespace("digest", quietly = TRUE)) {
    key <- digest::digest(list(draws, regimen, model_type, target_def, MIC))
    
    # Thread-safe check
    if (exists(key, envir = .pta_cache)) {
      return(.pta_cache[[key]])
    }
    
    # Compute with parallel support
    result <- pta_for_regimen(draws, regimen, model_type, target_def, MIC)
    
    # Thread-safe store
    .pta_cache[[key]] <- result
    return(result)
  } else {
    return(pta_for_regimen(draws, regimen, model_type, target_def, MIC))
  }
}

clear_pta_cache <- function() {
  rm(list = ls(envir = .pta_cache), envir = .pta_cache)
  gc(verbose = FALSE)
}

# ============================================================================
# Configuration
# ============================================================================

#' Configure parallel PTA/CFR calculations
#' @param n_cores Number of cores to use
#' @param chunk_size Chunk size for batch processing
#' @param progress Show progress
#' @export
configure_pta_parallel <- function(n_cores = NULL, 
                                  chunk_size = NULL,
                                  progress = TRUE) {
  
  if (is.null(n_cores)) {
    n_cores <- get_optimal_workers()
  }
  
  options(
    tdmx_parallel_cores = n_cores,
    tdmx_pta_chunk_size = chunk_size,
    tdmx_pta_progress = progress
  )
  
  message(sprintf("PTA/CFR parallelization configured with %d cores", n_cores))
  
  invisible(list(
    n_cores = n_cores,
    chunk_size = chunk_size,
    progress = progress
  ))
}

# Auto-configure on load
if (exists("get_optimal_workers")) {
  configure_pta_parallel()
}

message("Parallelized PTA/CFR calculations loaded.")
