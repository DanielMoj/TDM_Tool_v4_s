# R/async_fits.R
# Asynchrone Stan-Fits mit Future/Promises für non-blocking UI
# Unterstützt CmdStanR's parallel chains und Progress-Reporting

# Dependencies check
if (!requireNamespace("future", quietly = TRUE)) {
  stop("Package 'future' required. Install with: install.packages('future')")
}
if (!requireNamespace("promises", quietly = TRUE)) {
  stop("Package 'promises' required. Install with: install.packages('promises')")
}
if (!requireNamespace("progressr", quietly = TRUE)) {
  warning("Package 'progressr' recommended. Install with: install.packages('progressr')")
}

# Source parallel utilities
if (file.exists("R/parallel_utils.R")) {
  source("R/parallel_utils.R")
}

# ============================================================================
# Async Stan Fit Wrapper
# ============================================================================

#' Asynchroner Stan-Fit mit Future
#' 
#' @param stan_code Stan model code oder Pfad
#' @param data Stan data list
#' @param params Parameter zum Monitoren
#' @param chains Anzahl Chains
#' @param iter_warmup Warmup Iterationen
#' @param iter_sampling Sampling Iterationen
#' @param parallel_chains Anzahl paralleler Chains
#' @param adapt_delta Adapt delta Parameter
#' @param max_treedepth Max treedepth
#' @param progress Progress callback function
#' @param model_id Model identifier für Caching
#' @return Promise mit Fit-Ergebnis
#' @export
fit_stan_async <- function(stan_code,
                          data,
                          params = NULL,
                          chains = 4,
                          iter_warmup = 1000,
                          iter_sampling = 1000,
                          parallel_chains = NULL,
                          adapt_delta = 0.8,
                          max_treedepth = 10,
                          progress = NULL,
                          model_id = NULL) {
  
  # Check for cmdstanr
  if (!requireNamespace("cmdstanr", quietly = TRUE)) {
    stop("Package 'cmdstanr' required. Install with: cmdstanr::install_cmdstan()")
  }
  
  # Auto-detect optimal parallel chains
  if (is.null(parallel_chains)) {
    parallel_chains <- min(chains, get_optimal_workers(task_type = "cpu"))
  }
  
  # Create promise
  promises::future_promise({
    
    # Progress setup
    if (!is.null(progress) && requireNamespace("progressr", quietly = TRUE)) {
      p <- progressr::progressor(steps = chains * (iter_warmup + iter_sampling))
    } else {
      p <- function(...) invisible(NULL)
    }
    
    # Compile model
    if (is.character(stan_code) && file.exists(stan_code)) {
      model_file <- stan_code
    } else {
      model_file <- cmdstanr::write_stan_file(stan_code)
    }
    
    model <- cmdstanr::cmdstan_model(model_file, quiet = FALSE)
    
    # Setup callbacks for progress
    callbacks <- list()
    if (!is.null(progress)) {
      callbacks$refresh <- function(chain_id, iteration, ...) {
        p(amount = 1, 
          message = sprintf("Chain %d: Iteration %d", chain_id, iteration))
        if (!is.null(progress)) {
          progress(chain_id, iteration, chains, iter_warmup + iter_sampling)
        }
      }
    }
    
    # Run sampling with parallel chains
    fit <- model$sample(
      data = data,
      chains = chains,
      parallel_chains = parallel_chains,
      iter_warmup = iter_warmup,
      iter_sampling = iter_sampling,
      adapt_delta = adapt_delta,
      max_treedepth = max_treedepth,
      refresh = 100,
      show_messages = TRUE,
      show_exceptions = TRUE
    )
    
    # Extract results
    draws <- posterior::as_draws_df(fit$draws(variables = params))
    summary <- fit$summary()
    diagnostics <- fit$diagnostic_summary()
    
    # Check convergence
    max_rhat <- max(summary$rhat, na.rm = TRUE)
    min_ess <- min(summary$ess_bulk, na.rm = TRUE)
    n_divergent <- sum(diagnostics$num_divergent)
    
    # Prepare result
    result <- list(
      fit = fit,
      draws = draws,
      summary = summary,
      diagnostics = diagnostics,
      converged = max_rhat < 1.01 && min_ess > 400,
      n_divergent = n_divergent,
      model_id = model_id,
      timestamp = Sys.time()
    )
    
    # Clean up large fit object if not needed
    if (getOption("tdmx_async_cleanup", TRUE)) {
      result$fit <- NULL
      gc(verbose = FALSE)
    }
    
    return(result)
    
  }, seed = TRUE)  # Enable reproducibility
}

# ============================================================================
# Multi-Model Async Fitting
# ============================================================================

#' Führt mehrere Stan-Fits parallel aus
#' 
#' @param models Liste von Model-Spezifikationen
#' @param common_data Gemeinsame Daten (optional)
#' @param max_parallel Maximale parallele Fits
#' @param combine_results Results kombinieren
#' @return Promise mit allen Ergebnissen
#' @export
fit_stan_multiple_async <- function(models,
                                   common_data = NULL,
                                   max_parallel = NULL,
                                   combine_results = TRUE) {
  
  if (is.null(max_parallel)) {
    max_parallel <- get_optimal_workers(task_type = "memory")
  }
  
  # Setup future plan
  current_plan <- future::plan()
  future::plan(future::multisession, workers = max_parallel)
  on.exit(future::plan(current_plan))
  
  # Create promises for each model
  fit_promises <- lapply(seq_along(models), function(i) {
    model_spec <- models[[i]]
    
    # Merge common data
    if (!is.null(common_data)) {
      model_spec$data <- c(model_spec$data, common_data)
    }
    
    # Add model ID
    model_spec$model_id <- model_spec$model_id %||% paste0("model_", i)
    
    # Create async fit
    do.call(fit_stan_async, model_spec)
  })
  
  # Combine all promises
  if (combine_results) {
    promises::promise_all(.list = fit_promises) %>%
      promises::then(function(results) {
        names(results) <- sapply(models, function(m) m$model_id %||% "unnamed")
        
        # Combine draws if requested
        if (getOption("tdmx_combine_draws", FALSE)) {
          all_draws <- lapply(results, function(r) r$draws)
          results$combined_draws <- do.call(rbind, all_draws)
        }
        
        # Model comparison metrics
        if (requireNamespace("loo", quietly = TRUE)) {
          results$model_comparison <- compare_models_async(results)
        }
        
        return(results)
      })
  } else {
    return(fit_promises)
  }
}

# ============================================================================
# Progress Reporting via progressr
# ============================================================================

#' Setup Progress-Handler für async fits
#' 
#' @param type Handler-Typ ("cli", "shiny", "txt")
#' @return Progress handler
#' @export
setup_async_progress <- function(type = "cli") {
  
  if (!requireNamespace("progressr", quietly = TRUE)) {
    warning("progressr not available, progress reporting disabled")
    return(NULL)
  }
  
  handler <- switch(type,
    "cli" = progressr::handler_cli,
    "shiny" = progressr::handler_shiny,
    "txt" = progressr::handler_txtprogressbar,
    progressr::handler_void  # Silent fallback
  )
  
  progressr::handlers(handler())
  
  return(handler)
}

#' Progress-Wrapper für Shiny
#' 
#' @param session Shiny session
#' @param output_id Output ID für Progress
#' @return Progress callback
#' @export
create_shiny_progress <- function(session, output_id = "fit_progress") {
  
  function(chain_id, iteration, n_chains, total_iter) {
    progress <- (chain_id - 1) / n_chains + 
                (iteration / total_iter) / n_chains
    
    session$sendCustomMessage("updateProgress", list(
      id = output_id,
      value = round(progress * 100),
      message = sprintf("Chain %d/%d: Iteration %d/%d", 
                       chain_id, n_chains, iteration, total_iter)
    ))
  }
}

# ============================================================================
# Adaptive Multi-Chain Management
# ============================================================================

#' Adaptives Chain-Management basierend auf Convergence
#' 
#' @param stan_code Stan model code
#' @param data Stan data
#' @param min_chains Minimale Anzahl Chains
#' @param max_chains Maximale Anzahl Chains
#' @param target_ess Target ESS
#' @param check_interval Iterations zwischen Checks
#' @return Fit mit optimaler Chain-Anzahl
#' @export
fit_stan_adaptive_chains <- function(stan_code,
                                    data,
                                    min_chains = 2,
                                    max_chains = 8,
                                    target_ess = 1000,
                                    check_interval = 500) {
  
  current_chains <- min_chains
  combined_fit <- NULL
  total_ess <- 0
  
  while (current_chains <= max_chains && total_ess < target_ess) {
    
    message(sprintf("Running %d chains...", current_chains))
    
    # Run async fit
    fit_promise <- fit_stan_async(
      stan_code = stan_code,
      data = data,
      chains = current_chains,
      iter_warmup = check_interval,
      iter_sampling = check_interval
    )
    
    # Wait for result
    fit_result <- promises::promise_resolve(fit_promise)
    
    # Check ESS
    min_ess <- min(fit_result$summary$ess_bulk, na.rm = TRUE)
    total_ess <- total_ess + min_ess
    
    # Combine fits
    if (is.null(combined_fit)) {
      combined_fit <- fit_result
    } else {
      combined_fit$draws <- rbind(combined_fit$draws, fit_result$draws)
      combined_fit$summary <- update_summary(combined_fit$draws)
    }
    
    # Check if target reached
    if (total_ess >= target_ess) {
      message(sprintf("Target ESS reached with %d chains", current_chains))
      break
    }
    
    # Increase chains
    current_chains <- min(current_chains * 2, max_chains)
  }
  
  return(combined_fit)
}

# ============================================================================
# Warmstart Integration
# ============================================================================

#' Async Fit mit Warmstart
#' 
#' @param stan_code Stan model code
#' @param data Stan data
#' @param warmstart_file Pfad zu Warmstart-Daten
#' @param ... Weitere Parameter für fit_stan_async
#' @return Promise mit Fit-Ergebnis
#' @export
fit_stan_async_warmstart <- function(stan_code,
                                    data,
                                    warmstart_file = NULL,
                                    ...) {
  
  # Load warmstart if available
  init_values <- NULL
  if (!is.null(warmstart_file) && file.exists(warmstart_file)) {
    warmstart <- readRDS(warmstart_file)
    
    # Extract last values as init
    if (!is.null(warmstart$draws)) {
      last_draw <- warmstart$draws[nrow(warmstart$draws), ]
      init_values <- as.list(last_draw)
    }
    
    message("Using warmstart from: ", warmstart_file)
  }
  
  # Run async fit with init
  fit_promise <- fit_stan_async(
    stan_code = stan_code,
    data = data,
    init = init_values,
    ...
  )
  
  # Save warmstart after completion
  promises::then(fit_promise, function(result) {
    if (!is.null(warmstart_file)) {
      saveRDS(result, warmstart_file)
      message("Warmstart saved to: ", warmstart_file)
    }
    return(result)
  })
}

# ============================================================================
# Model Comparison Utilities
# ============================================================================

#' Vergleicht mehrere Modelle asynchron
#' 
#' @param results Liste von Fit-Ergebnissen
#' @return Model comparison table
#' @export
compare_models_async <- function(results) {
  
  if (!requireNamespace("loo", quietly = TRUE)) {
    warning("Package 'loo' required for model comparison")
    return(NULL)
  }
  
  # Extract log-likelihood for each model
  loo_list <- lapply(results, function(res) {
    if ("log_lik" %in% names(res$draws)) {
      log_lik_matrix <- as.matrix(res$draws[, grep("log_lik", names(res$draws))])
      loo::loo(log_lik_matrix, r_eff = loo::relative_eff(log_lik_matrix))
    } else {
      NULL
    }
  })
  
  # Remove NULL entries
  loo_list <- loo_list[!sapply(loo_list, is.null)]
  
  if (length(loo_list) < 2) {
    return(NULL)
  }
  
  # Compare models
  comparison <- loo::loo_compare(loo_list)
  
  return(comparison)
}

# ============================================================================
# Resource Monitoring
# ============================================================================

#' Überwacht Ressourcen während async fits
#' 
#' @param interval Check-Intervall in Sekunden
#' @return Monitor-Funktion
#' @export
create_resource_monitor <- function(interval = 5) {
  
  monitoring <- TRUE
  stats <- list()
  
  monitor_fun <- function() {
    while (monitoring) {
      # CPU usage
      cpu_usage <- system("top -bn1 | grep 'Cpu(s)' | awk '{print $2}'", 
                          intern = TRUE, ignore.stderr = TRUE)
      
      # Memory usage
      mem_info <- gc()
      mem_used <- sum(mem_info[, 2])
      
      # Active futures
      if (requireNamespace("future", quietly = TRUE)) {
        n_futures <- length(future::futureOf())
      } else {
        n_futures <- NA
      }
      
      # Store stats
      current_stats <- list(
        timestamp = Sys.time(),
        cpu = cpu_usage,
        memory_mb = mem_used,
        active_futures = n_futures
      )
      
      stats[[length(stats) + 1]] <- current_stats
      
      Sys.sleep(interval)
    }
    
    return(stats)
  }
  
  # Start monitoring in background
  if (requireNamespace("future", quietly = TRUE)) {
    future::future(monitor_fun(), lazy = FALSE)
  }
  
  # Return control function
  list(
    stop = function() { monitoring <<- FALSE },
    get_stats = function() { stats }
  )
}

# ============================================================================
# Export Functions
# ============================================================================

#' Export-Liste aller Async-Fit-Funktionen
#' @export
async_fits <- list(
  fit_stan_async = fit_stan_async,
  fit_stan_multiple_async = fit_stan_multiple_async,
  fit_stan_adaptive_chains = fit_stan_adaptive_chains,
  fit_stan_async_warmstart = fit_stan_async_warmstart,
  setup_async_progress = setup_async_progress,
  create_shiny_progress = create_shiny_progress,
  compare_models_async = compare_models_async,
  create_resource_monitor = create_resource_monitor
)

# Setup default future plan
if (requireNamespace("future", quietly = TRUE)) {
  # Auto-detect best plan
  if (.Platform$OS.type == "unix") {
    future::plan(future::multicore, workers = get_optimal_workers())
  } else {
    future::plan(future::multisession, workers = get_optimal_workers())
  }
  
  message("Async fits configured with ", future::nbrOfWorkers(), " workers")
}
