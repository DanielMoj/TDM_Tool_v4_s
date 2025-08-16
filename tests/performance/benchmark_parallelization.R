# tests/performance/benchmark_parallelization.R
# Performance Benchmark für Parallelisierungs-Features
# Vergleicht Sequential vs Parallel Performance

library(microbenchmark)
library(ggplot2)

# Source required files
source("../../R/parallel_utils.R")
source("../../R/async_fits.R")
source("../../R/job_queue.R")
source("../../R/pta_cfr.R")

# ============================================================================
# Benchmark Configuration
# ============================================================================

# Detect system capabilities
system_info <- list(
  platform = .Platform$OS.type,
  cores_available = parallel::detectCores(),
  cores_physical = parallel::detectCores(logical = FALSE),
  memory_mb = as.numeric(system("awk '/MemTotal/ {print $2/1024}' /proc/meminfo", 
                                intern = TRUE, ignore.stderr = TRUE)),
  r_version = R.version.string
)

print("System Information:")
print(system_info)

# Configure test parameters
test_configs <- list(
  small = list(n_draws = 100, n_timepoints = 50),
  medium = list(n_draws = 500, n_timepoints = 100),
  large = list(n_draws = 1000, n_timepoints = 200),
  xlarge = list(n_draws = 5000, n_timepoints = 500)
)

# Number of cores to test
core_configs <- c(1, 2, 4, min(8, system_info$cores_available))

# ============================================================================
# Generate Test Data
# ============================================================================

generate_test_draws <- function(n) {
  data.frame(
    CL = rlnorm(n, log(5), 0.3),
    Vc = rlnorm(n, log(30), 0.3),
    Q1 = rlnorm(n, log(2), 0.2),
    Vp1 = rlnorm(n, log(20), 0.2),
    sigma_add = runif(n, 0.05, 0.2),
    sigma_prop = runif(n, 0.05, 0.15)
  )
}

# Standard regimen
test_regimen <- list(
  dose = 1000,
  tau = 8,
  tinf = 1,
  n_doses = 10,
  start_time = 0
)

# Target definition
test_target <- list(
  type = "fT>MIC",
  cutoff = 0.5
)

# MIC distribution
test_mic_dist <- data.frame(
  mic = c(0.25, 0.5, 1, 2, 4, 8),
  p = c(0.05, 0.10, 0.30, 0.35, 0.15, 0.05)
)

# ============================================================================
# Mock Functions (needed for PTA calculations)
# ============================================================================

predict_conc_grid <- function(times, regimen, theta, model_type) {
  # Simple 1-compartment model for testing
  k_el <- theta$CL / theta$Vc
  dose_rate <- regimen$dose / regimen$tinf
  
  # Concentration during infusion
  conc <- numeric(length(times))
  for (i in seq_along(times)) {
    t <- times[i]
    if (t <= regimen$tinf) {
      conc[i] <- dose_rate / theta$CL * (1 - exp(-k_el * t))
    } else {
      c_end <- dose_rate / theta$CL * (1 - exp(-k_el * regimen$tinf))
      conc[i] <- c_end * exp(-k_el * (t - regimen$tinf))
    }
  }
  return(conc)
}

apply_site_penetration <- function(conc, drug, site, config) {
  return(conc * 0.7)  # Simple factor for testing
}

load_tissue_cfg <- function(path) {
  return(list(factor = 0.7))
}

meets_target <- function(metrics, target_def) {
  if (target_def$type == "fT>MIC") {
    return(metrics$ft_gt_mic >= target_def$cutoff)
  } else if (target_def$type == "AUC/MIC") {
    return(metrics$auc24_over_mic >= target_def$cutoff)
  }
  return(TRUE)
}

# ============================================================================
# Benchmark 1: PTA Calculation (Sequential vs Parallel)
# ============================================================================

cat("\n========================================\n")
cat("BENCHMARK 1: PTA Calculation\n")
cat("========================================\n\n")

pta_results <- list()

for (size_name in names(test_configs)) {
  config <- test_configs[[size_name]]
  draws <- generate_test_draws(config$n_draws)
  
  cat(sprintf("\nDataset: %s (n=%d draws)\n", size_name, config$n_draws))
  cat("----------------------------------------\n")
  
  # Benchmark for different core counts
  for (n_cores in core_configs) {
    if (n_cores > system_info$cores_available) next
    
    # Configure parallel settings
    options(tdmx_parallel_cores = n_cores)
    
    # Run benchmark
    bench_result <- microbenchmark(
      pta = pta_parallel(
        draws = draws,
        regimen = test_regimen,
        model_type = "2C",
        target_def = test_target,
        MIC = 2,
        n_cores = n_cores,
        progress = FALSE
      ),
      times = 5
    )
    
    # Store results
    pta_results[[paste(size_name, n_cores, sep = "_")]] <- list(
      size = size_name,
      n_draws = config$n_draws,
      n_cores = n_cores,
      time_median = median(bench_result$time) / 1e9,  # Convert to seconds
      time_mean = mean(bench_result$time) / 1e9,
      time_sd = sd(bench_result$time) / 1e9
    )
    
    cat(sprintf("  %d cores: %.3f sec (±%.3f)\n", 
               n_cores,
               mean(bench_result$time) / 1e9,
               sd(bench_result$time) / 1e9))
  }
}

# Calculate speedups
cat("\nSpeedup Analysis:\n")
cat("----------------------------------------\n")

for (size_name in names(test_configs)) {
  baseline_key <- paste(size_name, "1", sep = "_")
  if (!baseline_key %in% names(pta_results)) next
  
  baseline_time <- pta_results[[baseline_key]]$time_mean
  
  cat(sprintf("\n%s dataset:\n", size_name))
  
  for (n_cores in core_configs[-1]) {
    key <- paste(size_name, n_cores, sep = "_")
    if (!key %in% names(pta_results)) next
    
    parallel_time <- pta_results[[key]]$time_mean
    speedup <- baseline_time / parallel_time
    efficiency <- speedup / n_cores * 100
    
    cat(sprintf("  %d cores: %.2fx speedup (%.1f%% efficiency)\n", 
               n_cores, speedup, efficiency))
  }
}

# ============================================================================
# Benchmark 2: CFR Calculation with MIC Distribution
# ============================================================================

cat("\n========================================\n")
cat("BENCHMARK 2: CFR Calculation\n")
cat("========================================\n\n")

cfr_results <- list()

# Use medium dataset for CFR
draws <- generate_test_draws(500)

for (n_cores in core_configs) {
  if (n_cores > system_info$cores_available) next
  
  cat(sprintf("Testing with %d cores...\n", n_cores))
  
  bench_result <- microbenchmark(
    cfr = cfr_parallel(
      draws = draws,
      regimen = test_regimen,
      model_type = "2C",
      target_def = test_target,
      mic_dist = test_mic_dist,
      n_cores = n_cores,
      parallel_mic = TRUE
    ),
    times = 3
  )
  
  cfr_results[[as.character(n_cores)]] <- list(
    n_cores = n_cores,
    time_mean = mean(bench_result$time) / 1e9,
    time_sd = sd(bench_result$time) / 1e9
  )
}

# Show CFR results
cat("\nCFR Performance:\n")
cat("----------------------------------------\n")

baseline_cfr <- cfr_results[["1"]]$time_mean

for (n_cores in core_configs) {
  if (!as.character(n_cores) %in% names(cfr_results)) next
  
  result <- cfr_results[[as.character(n_cores)]]
  speedup <- baseline_cfr / result$time_mean
  
  cat(sprintf("  %d cores: %.3f sec (speedup: %.2fx)\n", 
             n_cores, result$time_mean, speedup))
}

# ============================================================================
# Benchmark 3: Job Queue Throughput
# ============================================================================

cat("\n========================================\n")
cat("BENCHMARK 3: Job Queue Throughput\n")
cat("========================================\n\n")

# Test job queue with different worker counts
job_queue_results <- list()

for (n_workers in c(1, 2, 4)) {
  if (n_workers > system_info$cores_available) next
  
  cat(sprintf("Testing with %d workers...\n", n_workers))
  
  # Create queue
  queue <- JobQueue$new(max_workers = n_workers, cache_dir = tempdir())
  
  # Submit multiple jobs
  n_jobs <- 20
  job_ids <- character(n_jobs)
  
  start_time <- Sys.time()
  
  for (i in 1:n_jobs) {
    job_ids[i] <- queue$submit(
      type = "batch_processing",
      spec = list(
        data = 1:100,
        fun = function(x) {
          Sys.sleep(0.1)  # Simulate work
          x^2
        }
      ),
      priority = sample(1:10, 1)
    )
  }
  
  # Wait for all jobs
  queue$wait_all(timeout = 60)
  
  end_time <- Sys.time()
  total_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
  
  # Get statistics
  stats <- queue$get_stats()
  
  job_queue_results[[as.character(n_workers)]] <- list(
    n_workers = n_workers,
    n_jobs = n_jobs,
    total_time = total_time,
    throughput = n_jobs / total_time,
    completed = stats$completed,
    failed = stats$failed,
    cache_hits = stats$cache_hits
  )
  
  cat(sprintf("  Completed %d jobs in %.2f sec (%.2f jobs/sec)\n",
             stats$completed, total_time, n_jobs / total_time))
}

# ============================================================================
# Benchmark 4: Parallel Diagnostics
# ============================================================================

cat("\n========================================\n")
cat("BENCHMARK 4: Parallel Diagnostics\n")
cat("========================================\n\n")

# Large draws for diagnostics
large_draws <- generate_test_draws(2000)

diag_results <- list()

for (n_cores in core_configs) {
  if (n_cores > system_info$cores_available) next
  
  cat(sprintf("Testing with %d cores...\n", n_cores))
  
  bench_result <- microbenchmark(
    diagnostics = calculate_diagnostics_parallel(large_draws, n_cores),
    times = 3
  )
  
  diag_results[[as.character(n_cores)]] <- list(
    n_cores = n_cores,
    time_mean = mean(bench_result$time) / 1e9,
    time_sd = sd(bench_result$time) / 1e9
  )
}

# Show diagnostics results
cat("\nDiagnostics Performance:\n")
cat("----------------------------------------\n")

baseline_diag <- diag_results[["1"]]$time_mean

for (n_cores in core_configs) {
  if (!as.character(n_cores) %in% names(diag_results)) next
  
  result <- diag_results[[as.character(n_cores)]]
  speedup <- baseline_diag / result$time_mean
  
  cat(sprintf("  %d cores: %.3f sec (speedup: %.2fx)\n", 
             n_cores, result$time_mean, speedup))
}

# ============================================================================
# Generate Performance Report
# ============================================================================

cat("\n========================================\n")
cat("PERFORMANCE REPORT SUMMARY\n")
cat("========================================\n\n")

# Create summary data frame
summary_data <- data.frame(
  Test = character(),
  Dataset = character(),
  Cores = integer(),
  Time_sec = numeric(),
  Speedup = numeric(),
  Efficiency = numeric(),
  stringsAsFactors = FALSE
)

# Add PTA results
for (key in names(pta_results)) {
  result <- pta_results[[key]]
  baseline_time <- pta_results[[paste(result$size, "1", sep = "_")]]$time_mean
  
  summary_data <- rbind(summary_data, data.frame(
    Test = "PTA",
    Dataset = result$size,
    Cores = result$n_cores,
    Time_sec = result$time_mean,
    Speedup = baseline_time / result$time_mean,
    Efficiency = (baseline_time / result$time_mean) / result$n_cores * 100,
    stringsAsFactors = FALSE
  ))
}

# Print summary table
print(summary_data)

# ============================================================================
# Visualization (if ggplot2 available)
# ============================================================================

if (requireNamespace("ggplot2", quietly = TRUE)) {
  
  # Speedup plot
  p1 <- ggplot(summary_data, aes(x = Cores, y = Speedup, color = Dataset)) +
    geom_line(size = 1.2) +
    geom_point(size = 3) +
    geom_abline(intercept = 0, slope = 1, linetype = "dashed", alpha = 0.5) +
    scale_x_continuous(breaks = core_configs) +
    labs(
      title = "Parallelization Speedup",
      subtitle = "PTA Calculation Performance",
      x = "Number of Cores",
      y = "Speedup Factor",
      color = "Dataset Size"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 14, face = "bold"),
      legend.position = "bottom"
    )
  
  print(p1)
  
  # Efficiency plot
  p2 <- ggplot(summary_data, aes(x = Cores, y = Efficiency, color = Dataset)) +
    geom_line(size = 1.2) +
    geom_point(size = 3) +
    geom_hline(yintercept = 100, linetype = "dashed", alpha = 0.5) +
    scale_x_continuous(breaks = core_configs) +
    scale_y_continuous(limits = c(0, 110)) +
    labs(
      title = "Parallel Efficiency",
      subtitle = "Percentage of Theoretical Maximum Speedup",
      x = "Number of Cores",
      y = "Efficiency (%)",
      color = "Dataset Size"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 14, face = "bold"),
      legend.position = "bottom"
    )
  
  print(p2)
  
  # Save plots
  ggsave("benchmark_speedup.png", p1, width = 10, height = 6, dpi = 150)
  ggsave("benchmark_efficiency.png", p2, width = 10, height = 6, dpi = 150)
  
  cat("\nPlots saved as benchmark_speedup.png and benchmark_efficiency.png\n")
}

# ============================================================================
# Save Results
# ============================================================================

# Save all results to RDS
all_results <- list(
  system_info = system_info,
  timestamp = Sys.time(),
  pta_results = pta_results,
  cfr_results = cfr_results,
  job_queue_results = job_queue_results,
  diag_results = diag_results,
  summary = summary_data
)

saveRDS(all_results, "benchmark_results.rds")
cat("\nResults saved to benchmark_results.rds\n")

# ============================================================================
# Final Summary
# ============================================================================

cat("\n========================================\n")
cat("BENCHMARK COMPLETE\n")
cat("========================================\n\n")

# Best speedups
best_pta_speedup <- max(summary_data$Speedup[summary_data$Test == "PTA"])
best_efficiency <- max(summary_data$Efficiency[summary_data$Test == "PTA"])

cat(sprintf("Best PTA Speedup: %.2fx\n", best_pta_speedup))
cat(sprintf("Best Efficiency: %.1f%%\n", best_efficiency))
cat(sprintf("Optimal Core Count: %d\n", 
           summary_data$Cores[which.max(summary_data$Efficiency)]))

# Recommendations
cat("\nRecommendations:\n")
cat("----------------\n")

if (best_efficiency > 80) {
  cat("✓ Excellent parallel efficiency achieved\n")
} else if (best_efficiency > 60) {
  cat("✓ Good parallel efficiency, consider optimizing chunk sizes\n")
} else {
  cat("⚠ Low parallel efficiency, check for bottlenecks\n")
}

if (system_info$cores_available > 4) {
  cat("✓ System has sufficient cores for effective parallelization\n")
} else {
  cat("ℹ Limited cores available, consider cloud/cluster computing for large jobs\n")
}

cat("\n✓ Benchmark completed successfully!\n")
