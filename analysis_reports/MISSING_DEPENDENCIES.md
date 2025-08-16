# Fehlende Abhängigkeiten

Anzahl fehlend: **27**

| Quelle | Ziel | Typ |
|---|---|---|
| `.github/workflows/ci.yml` | `.github/workflows/Rscript tests/performance/benchmark.R
` | `references` |
| `.github/workflows/ci.yml` | `.github/workflows/benchmark-results.json` | `references` |
| `Dockerfile.api` | `rocker/r-ver:4.3.1` | `inherits_from` |
| `Dockerfile.app` | `rocker/shiny:4.3.1` | `inherits_from` |
| `Dockerfile.test` | `NAMESPACE` | `copies` |
| `Dockerfile.test` | `data` | `copies` |
| `Dockerfile.test` | `inst` | `copies` |
| `Dockerfile.test` | `man` | `copies` |
| `Dockerfile.test` | `rocker/verse:4.3.2` | `inherits_from` |
| `R/backend_bayes.R` | `R/config/optimization_config.yaml` | `loads_config` |
| `R/optimize_regimen.R` | `R/config/risk_models.json` | `reads_data` |
| `R/optimize_regimen.R` | `R/config/tissue.json` | `reads_data` |
| `R/pta_cfr.R` | `R/config/tissue.json` | `reads_data` |
| `docker-compose.test.yml` | `K6_OUT=json=/results/load-test.json` | `references` |
| `examples/parallel_usage_example.R` | `examples/config/optimization_config.yaml` | `loads_config` |
| `tests/performance/benchmark.R` | `tests/performance/R/pk_calculations.R` | `sources` |
| `tests/performance/benchmark.R` | `tests/performance/R/plotting_functions.R` | `sources` |
| `tests/testthat.R` | `tests/tests/testthat/fixtures/test-data.R` | `sources` |
| `tests/testthat/helper-common.R` | `tests/testthat/R/utils.R` | `sources` |
| `tests/testthat/test-db-error-handling.R` | `tests/testthat/R/antibiogram.R` | `sources` |
| `tests/testthat/test-db-error-handling.R` | `tests/testthat/R/audit.R` | `sources` |
| `tests/testthat/test-db-error-handling.R` | `tests/testthat/R/db.R` | `sources` |
| `tests/testthat/test-db-error-handling.R` | `tests/testthat/R/utils.R` | `sources` |
| `tests/testthat/test-db-error-handling.R` | `tests/testthat/nonexistent.csv` | `reads_data` |
| `tests/testthat/test-error-handling-integration.R` | `tests/testthat/missing.yaml` | `loads_config` |
| `tests/testthat/test-error-handling-integration.R` | `tests/testthat/non_existent_file.txt` | `reads_data` |
| `tests/testthat/test-utils.R` | `tests/testthat/R/utils.R` | `sources` |