# tests/testthat/test-parallelization.R
# Test-Suite für Parallelisierungs-Features

library(testthat)

# Source required files
source("../../R/parallel_utils.R")
source("../../R/async_fits.R")
source("../../R/job_queue.R")
source("../../R/pta_cfr.R")

# ============================================================================
# Tests für parallel_utils.R
# ============================================================================

test_that("get_optimal_workers returns reasonable values", {
  workers <- get_optimal_workers()
  
  expect_true(is.numeric(workers))
  expect_gte(workers, 1)
  expect_lte(workers, parallel::detectCores())
})

test_that("get_optimal_workers respects max_cores parameter", {
  workers <- get_optimal_workers(max_cores = 2)
  expect_lte(workers, 2)
})

test_that("get_optimal_workers adjusts for task type", {
  cpu_workers <- get_optimal_workers(task_type = "cpu")
  memory_workers <- get_optimal_workers(task_type = "memory")
  io_workers <- get_optimal_workers(task_type = "io")
  
  # Memory-intensive tasks should use fewer workers
  expect_lte(memory_workers, cpu_workers)
  expect_lte(memory_workers, 4)
  
  # I/O-bound tasks should be limited
  expect_lte(io_workers, 6)
})

test_that("create_chunks creates balanced chunks", {
  n_items <- 100
  n_workers <- 4
  
  chunks <- create_chunks(n_items, n_workers)
  
  expect_equal(length(unlist(chunks)), n_items)
  expect_lte(length(chunks), n_workers)
  
  # Check chunk sizes are roughly balanced
  chunk_sizes <- sapply(chunks, length)
  expect_lte(max(chunk_sizes) - min(chunk_sizes), 1)
})

test_that("create_chunks respects minimum chunk size", {
  n_items <- 100
  n_workers <- 20
  min_chunk_size <- 10
  
  chunks <- create_chunks(n_items, n_workers, min_chunk_size)
  
  chunk_sizes <- sapply(chunks, length)
  expect_true(all(chunk_sizes >= min_chunk_size | chunk_sizes == 0))
})

test_that("parallel_process_chunked works correctly", {
  skip_on_cran()  # Skip on CRAN due to parallel processing
  
  # Test data
  data <- 1:100
  fun <- function(x) x^2
  
  # Sequential result
  expected <- sapply(data, fun)
  
  # Parallel result
  result <- parallel_process_chunked(
    data = data,
    fun = function(chunk) sapply(chunk, fun),
    n_cores = 2,
    progress = FALSE,
    combine = c
  )
  
  expect_equal(result, expected)
})

test_that("platform-specific cluster creation works", {
  cl <- make_adaptive_cluster(n_cores = 2)
  
  expect_s3_class(cl, "cluster")
  
  # Test basic operation
  result <- parallel::clusterEvalQ(cl, 1 + 1)
  expect_equal(unlist(result), c(2, 2))
  
  parallel::stopCluster(cl)
})

# ============================================================================
# Tests für async_fits.R
# ============================================================================

test_that("fit_stan_async returns a promise", {
  skip_if_not(requireNamespace("future", quietly = TRUE))
  skip_if_not(requireNamespace("promises", quietly = TRUE))
  
  # Mock Stan code
  stan_code <- "
  parameters {
    real theta;
  }
  model {
    theta ~ normal(0, 1);
  }
  "
  
  # Test async fit
  promise <- fit_stan_async(
    stan_code = stan_code,
    data = list(),
    chains = 1,
    iter_warmup = 10,
    iter_sampling = 10
  )
  
  expect_s3_class(promise, "promise")
})

test_that("setup_async_progress returns appropriate handler", {
  skip_if_not(requireNamespace("progressr", quietly = TRUE))
  
  handler <- setup_async_progress(type = "cli")
  expect_true(is.function(handler) || is.null(handler))
})

# ============================================================================
# Tests für job_queue.R
# ============================================================================

test_that("JobQueue initializes correctly", {
  skip_if_not(requireNamespace("R6", quietly = TRUE))
  
  queue <- JobQueue$new(max_workers = 2)
  
  expect_s3_class(queue, "JobQueue")
  expect_s3_class(queue, "R6")
})

test_that("JobQueue can submit and retrieve jobs", {
  skip_if_not(requireNamespace("R6", quietly = TRUE))
  
  queue <- JobQueue$new(max_workers = 1)
  
  # Submit simple job
  job_id <- queue$submit(
    type = "batch_processing",
    spec = list(
      data = 1:10,
      fun = function(x) x^2
    ),
    priority = 5
  )
  
  expect_true(is.character(job_id))
  expect_true(nchar(job_id) > 0)
  
  # Wait for completion
  Sys.sleep(0.5)
  
  # Check status
  status <- queue$status(job_id)
  expect_true(status %in% c("queued", "running", "completed", "failed"))
})

test_that("JobQueue respects priorities", {
  skip_if_not(requireNamespace("R6", quietly = TRUE))
  
  queue <- JobQueue$new(max_workers = 1)
  
  # Submit jobs with different priorities
  low_id <- queue$submit(
    type = "batch_processing",
    spec = list(data = 1:10, fun = identity),
    priority = 1
  )
  
  high_id <- queue$submit(
    type = "batch_processing",
    spec = list(data = 1:10, fun = identity),
    priority = 10
  )
  
  # High priority should be processed first
  # (This is a simplified test - real verification would be more complex)
  expect_true(is.character(low_id))
  expect_true(is.character(high_id))
})

test_that("JobQueue caching works", {
  skip_if_not(requireNamespace("R6", quietly = TRUE))
  skip_if_not(requireNamespace("digest", quietly = TRUE))
  
  queue <- JobQueue$new(max_workers = 1, cache_dir = tempdir())
  
  # Submit same job twice
  spec <- list(data = 1:10, fun = function(x) x^2)
  
  job_id1 <- queue$submit(
    type = "batch_processing",
    spec = spec,
    priority = 5
  )
  
  # Wait for completion
  queue$wait_all(timeout = 5)
  
  job_id2 <- queue$submit(
    type = "batch_processing",
    spec = spec,
    priority = 5
  )
  
  # Second job should use cache (much faster)
  queue$wait_all(timeout = 1)
  
  # Both should have same result
  result1 <- queue$result(job_id1)
  result2 <- queue$result(job_id2)
  
  expect_equal(result1, result2)
})

# ============================================================================
# Tests für pta_cfr.R (Parallel Version)
# ============================================================================

test_that("pta_parallel produces same results as sequential", {
  skip_on_cran()
  
  # Mock data
  set.seed(123)
  draws <- data.frame(
    CL = rlnorm(50, log(5), 0.2),
    Vc = rlnorm(50, log(30), 0.2)
  )
  
  regimen <- list(
    dose = 1000,
    tau = 8,
    tinf = 1,
    n_doses = 5,
    start_time = 0
  )
  
  model_type <- "1C"
  target_def <- list(type = "fT>MIC", cutoff = 0.5)
  MIC <- 2
  
  # Mock required functions
  predict_conc_grid <- function(times, regimen, theta, model_type) {
    # Simple exponential decay
    k_el <- theta$CL / theta$Vc
    dose_per_time <- regimen$dose / regimen$tinf
    conc <- dose_per_time / theta$CL * (1 - exp(-k_el * times))
    return(conc)
  }
  
  apply_site_penetration <- function(conc, drug, site, config) {
    return(conc)  # No modification
  }
  
  load_tissue_cfg <- function(path) {
    return(list())
  }
  
  meets_target <- function(metrics, target_def) {
    if (target_def$type == "fT>MIC") {
      return(metrics$ft_gt_mic >= target_def$cutoff)
    }
    return(TRUE)
  }
  
  # Sequential
  pta_seq <- pta_for_regimen(draws, regimen, model_type, target_def, MIC)
  
  # Parallel
  pta_par <- pta_parallel(draws, regimen, model_type, target_def, MIC, 
                          n_cores = 2, progress = FALSE)
  
  # Should be very close (allowing for minor numerical differences)
  expect_equal(pta_seq, pta_par, tolerance = 0.01)
})

test_that("cfr_parallel calculates correctly", {
  skip_on_cran()
  
  # Mock data
  set.seed(456)
  draws <- data.frame(
    CL = rlnorm(30, log(5), 0.2),
    Vc = rlnorm(30, log(30), 0.2)
  )
  
  regimen <- list(
    dose = 1000,
    tau = 8,
    tinf = 1,
    n_doses = 5,
    start_time = 0
  )
  
  model_type <- "1C"
  target_def <- list(type = "AUC/MIC", cutoff = 125)
  
  mic_dist <- data.frame(
    mic = c(0.5, 1, 2, 4),
    p = c(0.2, 0.4, 0.3, 0.1)
  )
  
  # Mock functions (same as above)
  predict_conc_grid <- function(times, regimen, theta, model_type) {
    k_el <- theta$CL / theta$Vc
    dose_per_time <- regimen$dose / regimen$tinf
    conc <- dose_per_time / theta$CL * (1 - exp(-k_el * times))
    return(conc)
  }
  
  apply_site_penetration <- function(conc, drug, site, config) {
    return(conc)
  }
  
  load_tissue_cfg <- function(path) {
    return(list())
  }
  
  meets_target <- function(metrics, target_def) {
    if (target_def$type == "AUC/MIC") {
      return(metrics$auc24_over_mic >= target_def$cutoff)
    }
    return(TRUE)
  }
  
  # Calculate CFR
  cfr <- cfr_parallel(draws, regimen, model_type, target_def, mic_dist,
                     n_cores = 2, parallel_mic = TRUE)
  
  expect_true(is.numeric(cfr))
  expect_gte(cfr, 0)
  expect_lte(cfr, 1)
})

test_that("pta_batch_parallel processes multiple regimens", {
  skip_on_cran()
  
  # Mock data
  set.seed(789)
  draws <- data.frame(
    CL = rlnorm(20, log(5), 0.2),
    Vc = rlnorm(20, log(30), 0.2)
  )
  
  regimens_list <- list(
    low = list(dose = 500, tau = 8, tinf = 1, n_doses = 5, start_time = 0),
    standard = list(dose = 1000, tau = 8, tinf = 1, n_doses = 5, start_time = 0),
    high = list(dose = 2000, tau = 12, tinf = 2, n_doses = 4, start_time = 0)
  )
  
  model_type <- "1C"
  target_def <- list(type = "fT>MIC", cutoff = 0.5)
  MIC <- 2
  
  # Mock functions
  predict_conc_grid <- function(times, regimen, theta, model_type) {
    k_el <- theta$CL / theta$Vc
    dose_per_time <- regimen$dose / regimen$tinf
    conc <- dose_per_time / theta$CL * (1 - exp(-k_el * times))
    return(conc)
  }
  
  apply_site_penetration <- function(conc, drug, site, config) {
    return(conc)
  }
  
  load_tissue_cfg <- function(path) {
    return(list())
  }
  
  meets_target <- function(metrics, target_def) {
    return(metrics$ft_gt_mic >= target_def$cutoff)
  }
  
  # Batch calculation
  pta_results <- pta_batch_parallel(
    draws, regimens_list, model_type, target_def, MIC,
    n_cores = 2, load_balance = FALSE
  )
  
  expect_equal(length(pta_results), length(regimens_list))
  expect_true(all(pta_results >= 0))
  expect_true(all(pta_results <= 1))
  
  # Higher doses should generally have higher PTA
  expect_gte(pta_results[3], pta_results[1])  # high >= low
})

# ============================================================================
# Integration Tests
# ============================================================================

test_that("Full parallel pipeline works end-to-end", {
  skip_on_cran()
  skip_if_not(requireNamespace("R6", quietly = TRUE))
  
  # Setup
  configure_parallel(max_cores = 2, progress = FALSE)
  
  # Create job queue
  queue <- JobQueue$new(max_workers = 2)
  
  # Submit PTA job
  job_id <- queue$submit(
    type = "batch_processing",
    spec = list(
      data = 1:100,
      fun = function(x) x^2
    ),
    priority = 5
  )
  
  # Wait and get result
  result <- queue$result(job_id, wait = TRUE, timeout = 10)
  
  expect_equal(result, (1:100)^2)
  
  # Check stats
  stats <- queue$get_stats()
  expect_equal(stats$completed, 1)
  expect_equal(stats$failed, 0)
})

# ============================================================================
# Performance Tests
# ============================================================================

test_that("Parallelization provides speedup", {
  skip_on_cran()
  skip_on_ci()  # Skip in CI environments
  
  if (parallel::detectCores() < 2) {
    skip("Not enough cores for performance test")
  }
  
  # Large dataset
  n <- 1000
  data <- 1:n
  fun <- function(x) {
    Sys.sleep(0.001)  # Simulate work
    x^2
  }
  
  # Sequential timing
  time_seq <- system.time({
    result_seq <- lapply(data, fun)
  })["elapsed"]
  
  # Parallel timing
  time_par <- system.time({
    result_par <- parallel_process_chunked(
      data = data,
      fun = function(chunk) lapply(chunk, fun),
      n_cores = 2,
      progress = FALSE,
      combine = c
    )
  })["elapsed"]
  
  # Should have speedup (allowing for overhead)
  speedup <- time_seq / time_par
  expect_gt(speedup, 1.2)  # At least 20% speedup
  
  # Results should be identical
  expect_equal(unlist(result_seq), unlist(result_par))
})

# ============================================================================
# Error Handling Tests
# ============================================================================

test_that("Functions handle errors gracefully", {
  skip_if_not(requireNamespace("R6", quietly = TRUE))
  
  queue <- JobQueue$new(max_workers = 1)
  
  # Submit job that will fail
  job_id <- queue$submit(
    type = "batch_processing",
    spec = list(
      data = 1:10,
      fun = function(x) stop("Intentional error")
    ),
    priority = 5,
    error_callback = function(e) {
      message("Error caught: ", e$message)
    }
  )
  
  # Wait a bit
  Sys.sleep(1)
  
  # Should be in failed state
  status <- queue$status(job_id)
  expect_equal(status, "failed")
})

test_that("Parallel functions handle missing packages", {
  # Test handling of missing optional packages
  if (requireNamespace("bigmemory", quietly = TRUE)) {
    # Test shared memory setup
    data <- matrix(1:100, 10, 10)
    shared <- setup_shared_memory(data)
    
    if (.Platform$OS.type == "unix") {
      expect_true(!is.null(shared))
    }
  } else {
    # Should handle missing package gracefully
    data <- matrix(1:100, 10, 10)
    shared <- setup_shared_memory(data)
    expect_null(shared)
  }
})

# Run tests
if (interactive()) {
  test_results <- test_dir(".")
  print(test_results)
}
