# examples/parallel_usage_example.R
# Beispiele für die Verwendung der Parallelisierungs-Features
# TDMx Repository - Work Package 5

# ============================================================================
# Setup und Initialisierung
# ============================================================================

# Load required sources
source("R/parallel_utils.R")
source("R/async_fits.R")
source("R/job_queue.R")
source("R/pta_cfr.R")
source("R/backend_bayes.R")

# Load configuration
config <- yaml::read_yaml("config/optimization_config.yaml")

# Configure parallel processing
configure_parallel(
  max_cores = config$parallel$max_cores,
  method = config$parallel$method,
  progress = config$parallel$show_progress
)

# ============================================================================
# BEISPIEL 1: Parallelisierte PTA-Berechnung
# ============================================================================

cat("\n=== BEISPIEL 1: Parallelisierte PTA-Berechnung ===\n\n")

# Simuliere Posterior Draws (normalerweise von Stan/MCMC)
set.seed(123)
n_draws <- 1000
draws <- data.frame(
  CL = rlnorm(n_draws, log(5), 0.3),
  Vc = rlnorm(n_draws, log(30), 0.3),
  Q1 = rlnorm(n_draws, log(2), 0.2),
  Vp1 = rlnorm(n_draws, log(20), 0.2),
  sigma_add = runif(n_draws, 0.05, 0.2),
  sigma_prop = runif(n_draws, 0.05, 0.15)
)

# Dosierungsschema
regimen <- list(
  dose = 1000,    # mg
  tau = 8,        # hours
  tinf = 1,       # hours infusion
  n_doses = 10,
  start_time = 0
)

# Target-Definition
target_def <- list(
  type = "fT>MIC",
  cutoff = 0.5    # 50% time above MIC
)

# MIC
MIC <- 2  # mg/L

# Zeitmessung: Sequentiell vs Parallel
cat("Vergleiche Sequentiell vs Parallel PTA:\n")

# Sequentiell (single core)
time_seq <- system.time({
  pta_seq <- pta_for_regimen(draws[1:100,], regimen, "2C", target_def, MIC)
})
cat(sprintf("Sequentiell (100 draws): %.2f Sekunden, PTA = %.2f%%\n", 
            time_seq["elapsed"], pta_seq * 100))

# Parallel (multi-core)
time_par <- system.time({
  pta_par <- pta_parallel(draws, regimen, "2C", target_def, MIC, 
                          n_cores = 4, progress = FALSE)
})
cat(sprintf("Parallel (1000 draws, 4 cores): %.2f Sekunden, PTA = %.2f%%\n", 
            time_par["elapsed"], pta_par * 100))

speedup <- (time_seq["elapsed"] * 10) / time_par["elapsed"]  # Adjusted for 10x data
cat(sprintf("Speedup: %.1fx\n\n", speedup))

# ============================================================================
# BEISPIEL 2: Asynchrone Stan-Fits
# ============================================================================

cat("=== BEISPIEL 2: Asynchrone Stan-Fits ===\n\n")

# Stan model code (einfaches PK-Modell)
stan_code <- "
data {
  int<lower=0> N;
  vector[N] time;
  vector[N] conc;
}
parameters {
  real<lower=0> CL;
  real<lower=0> Vc;
  real<lower=0> sigma;
}
model {
  CL ~ lognormal(log(5), 0.3);
  Vc ~ lognormal(log(30), 0.3);
  sigma ~ normal(0, 1);
  
  vector[N] pred;
  for (i in 1:N) {
    pred[i] = 1000 / Vc * exp(-CL/Vc * time[i]);
  }
  conc ~ normal(pred, sigma);
}
"

# Simuliere Daten
obs_data <- list(
  N = 10,
  time = c(0.5, 1, 2, 4, 6, 8, 12, 16, 20, 24),
  conc = c(28, 25, 20, 15, 12, 10, 7, 5, 4, 3) + rnorm(10, 0, 2)
)

# Async fit mit Progress-Reporting
cat("Starte asynchronen Stan-Fit...\n")

# Create progress callback
progress_callback <- function(chain, iter, n_chains, total_iter) {
  progress <- (chain - 1) / n_chains + (iter / total_iter) / n_chains
  cat(sprintf("\rProgress: %3.0f%% [Chain %d/%d, Iteration %d/%d]", 
              progress * 100, chain, n_chains, iter, total_iter))
}

# Start async fit
fit_promise <- fit_stan_async(
  stan_code = stan_code,
  data = obs_data,
  chains = 2,
  iter_warmup = 500,
  iter_sampling = 500,
  parallel_chains = 2,
  progress = progress_callback,
  model_id = "example_pk_model"
)

# Simulate other work while fit runs
cat("\n\nFühre andere Berechnungen aus während Stan läuft...\n")
Sys.sleep(1)
cat("Andere Arbeit erledigt!\n\n")

# Wait for result
cat("Warte auf Stan-Ergebnis...\n")
fit_result <- promises::promise_resolve(fit_promise)

cat(sprintf("Fit abgeschlossen! Converged: %s, Divergences: %d\n",
            fit_result$converged, fit_result$n_divergent))

# ============================================================================
# BEISPIEL 3: Job Queue System
# ============================================================================

cat("\n=== BEISPIEL 3: Job Queue System ===\n\n")

# Initialize job queue
queue <- JobQueue$new(max_workers = 2, cache_dir = tempdir())

# Submit multiple PTA jobs with different priorities
cat("Submitte multiple Jobs mit verschiedenen Prioritäten:\n")

job_ids <- character(5)

# High priority job
job_ids[1] <- queue$submit(
  type = "pta_calculation",
  spec = list(
    draws = draws[1:200,],
    regimen = regimen,
    model_type = "2C",
    target_def = target_def,
    MIC = 1
  ),
  priority = 10,  # Urgent
  callback = function(result) {
    cat(sprintf("\n✓ High priority job completed: PTA = %.2f%%\n", result * 100))
  }
)

# Normal priority jobs
for (i in 2:4) {
  job_ids[i] <- queue$submit(
    type = "pta_calculation",
    spec = list(
      draws = draws[1:200,],
      regimen = regimen,
      model_type = "2C",
      target_def = target_def,
      MIC = i
    ),
    priority = 5,  # Normal
    callback = function(result) {
      cat(sprintf("✓ Normal priority job completed: PTA = %.2f%%\n", result * 100))
    }
  )
}

# Low priority batch job
job_ids[5] <- queue$submit(
  type = "batch_processing",
  spec = list(
    data = 1:100,
    fun = function(x) x^2
  ),
  priority = 1,  # Low
  callback = function(result) {
    cat(sprintf("✓ Batch job completed: %d items processed\n", length(result)))
  }
)

# Monitor queue
cat("\nQueue Status:\n")
print(queue)

# Wait for specific job
cat("\nWarte auf High-Priority Job...\n")
high_priority_result <- queue$result(job_ids[1], wait = TRUE, timeout = 30)
cat(sprintf("High-Priority Ergebnis: PTA = %.2f%%\n", high_priority_result * 100))

# Get queue statistics
stats <- queue$get_stats()
cat("\nQueue Statistics:\n")
cat(sprintf("  Completed: %d\n", stats$completed))
cat(sprintf("  Failed: %d\n", stats$failed))
cat(sprintf("  Cache hits: %d\n", stats$cache_hits))
cat(sprintf("  Avg wait time: %.1f seconds\n", stats$avg_wait_time))
cat(sprintf("  Avg run time: %.1f seconds\n", stats$avg_run_time))

# ============================================================================
# BEISPIEL 4: Batch PTA für Multiple Regimens
# ============================================================================

cat("\n=== BEISPIEL 4: Batch PTA für Multiple Regimens ===\n\n")

# Erstelle verschiedene Dosierungsschemata
regimens_list <- list(
  standard = list(dose = 1000, tau = 8, tinf = 1, n_doses = 10, start_time = 0),
  high_dose = list(dose = 2000, tau = 12, tinf = 2, n_doses = 8, start_time = 0),
  continuous = list(dose = 3000, tau = 24, tinf = 24, n_doses = 5, start_time = 0),
  loading = list(dose = 1500, tau = 8, tinf = 0.5, n_doses = 10, start_time = 0),
  extended = list(dose = 1000, tau = 8, tinf = 4, n_doses = 10, start_time = 0)
)

cat("Berechne PTA für 5 verschiedene Dosierungsschemata...\n")

# Batch PTA calculation
time_batch <- system.time({
  pta_results <- pta_batch_parallel(
    draws = draws[1:500,],
    regimens_list = regimens_list,
    model_type = "2C",
    target_def = target_def,
    MIC = 2,
    n_cores = 4,
    load_balance = TRUE
  )
})

# Ergebnisse anzeigen
cat("\nPTA-Ergebnisse für verschiedene Regimes:\n")
for (i in seq_along(regimens_list)) {
  cat(sprintf("  %s: PTA = %.1f%%\n", 
              names(regimens_list)[i], 
              pta_results[i] * 100))
}
cat(sprintf("\nGesamtzeit für 5 Regimes: %.2f Sekunden\n", time_batch["elapsed"]))

# ============================================================================
# BEISPIEL 5: Parallel Diagnostics
# ============================================================================

cat("\n=== BEISPIEL 5: Parallel Diagnostics ===\n\n")

# Berechne Diagnostics parallel
cat("Berechne ESS und Rhat für alle Parameter...\n")

time_diag <- system.time({
  diagnostics <- calculate_diagnostics_parallel(draws, n_cores = 4)
})

# Zeige Ergebnisse
cat("\nDiagnostics Summary:\n")
cat(sprintf("  Min ESS (bulk): %.0f\n", min(diagnostics$ess_bulk)))
cat(sprintf("  Max Rhat: %.3f\n", max(diagnostics$rhat)))
cat(sprintf("  Zeit: %.2f Sekunden\n", time_diag["elapsed"]))

# Check convergence
converged <- all(diagnostics$rhat < 1.01) && all(diagnostics$ess_bulk > 400)
cat(sprintf("\nKonvergiert: %s\n", ifelse(converged, "✓ Ja", "✗ Nein")))

# ============================================================================
# BEISPIEL 6: CFR mit MIC-Verteilung
# ============================================================================

cat("\n=== BEISPIEL 6: CFR mit MIC-Verteilung ===\n\n")

# MIC-Verteilung (z.B. aus Antibiogramm)
mic_dist <- data.frame(
  mic = c(0.25, 0.5, 1, 2, 4, 8),
  p = c(0.05, 0.10, 0.30, 0.35, 0.15, 0.05)
)

cat("MIC-Verteilung:\n")
print(mic_dist)

# Berechne CFR parallel
cat("\nBerechne CFR...\n")

time_cfr <- system.time({
  cfr_result <- cfr_parallel(
    draws = draws[1:500,],
    regimen = regimen,
    model_type = "2C",
    target_def = target_def,
    mic_dist = mic_dist,
    n_cores = 4,
    parallel_mic = TRUE
  )
})

cat(sprintf("\nCFR = %.1f%%\n", cfr_result * 100))
cat(sprintf("Berechnungszeit: %.2f Sekunden\n", time_cfr["elapsed"]))

# ============================================================================
# Performance Summary
# ============================================================================

cat("\n" %+% paste(rep("=", 60), collapse = "") %+% "\n")
cat("PERFORMANCE SUMMARY\n")
cat(paste(rep("=", 60), collapse = "") %+% "\n\n")

# Get system info
cat("System Information:\n")
cat(sprintf("  Platform: %s\n", .Platform$OS.type))
cat(sprintf("  Cores available: %d\n", parallel::detectCores()))
cat(sprintf("  Cores used: %d\n", get_optimal_workers()))
cat(sprintf("  R version: %s\n", R.version.string))

# Performance metrics
cat("\nPerformance Metrics:\n")
cat(sprintf("  PTA calculation speedup: %.1fx\n", speedup))
cat(sprintf("  Jobs processed: %d\n", stats$completed))
cat(sprintf("  Cache efficiency: %.0f%%\n", 
            (stats$cache_hits / stats$completed) * 100))

# Memory usage
mem_info <- gc()
cat(sprintf("\nMemory Usage:\n"))
cat(sprintf("  Used: %.1f MB\n", sum(mem_info[, 2])))
cat(sprintf("  Max used: %.1f MB\n", sum(mem_info[, 6])))

cat("\n✓ Alle Beispiele erfolgreich ausgeführt!\n")
