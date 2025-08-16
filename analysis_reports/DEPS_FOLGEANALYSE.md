# Folgeanalyse der Abhängigkeiten

- **Gesamt-Knoten (Quellen)**: 41
- **Gesamt-Kanten**: 114
- **Fehlende Kanten**: 27 (23.7%)

## Top-Quellen mit fehlenden Abhängigkeiten

| Quelle | Fehlend |
|---|---:|
| `Dockerfile.test` | 5 |
| `tests/testthat/test-db-error-handling.R` | 5 |
| `R/optimize_regimen.R` | 2 |
| `.github/workflows/ci.yml` | 2 |
| `tests/performance/benchmark.R` | 2 |
| `tests/testthat/test-error-handling-integration.R` | 2 |
| `docker-compose.test.yml` | 1 |
| `Dockerfile.api` | 1 |
| `Dockerfile.app` | 1 |
| `examples/parallel_usage_example.R` | 1 |
| `R/backend_bayes.R` | 1 |
| `R/pta_cfr.R` | 1 |
| `tests/testthat.R` | 1 |
| `tests/testthat/helper-common.R` | 1 |
| `tests/testthat/test-utils.R` | 1 |

## Fehlende pro Typ

| Typ | Fehlend |
|---|---:|
| sources | 9 |
| reads_data | 5 |
| copies | 4 |
| references | 3 |
| inherits_from | 3 |
| loads_config | 3 |

## Artefakte

- Normalisierte JSON: `C:\Users\Danie\Meine Ablage (moba.apotheken@gmail.com)\00000000000000_UNTERNEHMENSGRUPPE\12_KOLUMBUS_BioComputing_GmbH\001_TDM_Tool\v6\analysis_reports\deps_normalized.json`
- Fehlende (CSV): `C:\Users\Danie\Meine Ablage (moba.apotheken@gmail.com)\00000000000000_UNTERNEHMENSGRUPPE\12_KOLUMBUS_BioComputing_GmbH\001_TDM_Tool\v6\analysis_reports\missing_dependencies.csv`
- Fehlende (Markdown): `C:\Users\Danie\Meine Ablage (moba.apotheken@gmail.com)\00000000000000_UNTERNEHMENSGRUPPE\12_KOLUMBUS_BioComputing_GmbH\001_TDM_Tool\v6\analysis_reports\MISSING_DEPENDENCIES.md`
- Stats (Typ): `C:\Users\Danie\Meine Ablage (moba.apotheken@gmail.com)\00000000000000_UNTERNEHMENSGRUPPE\12_KOLUMBUS_BioComputing_GmbH\001_TDM_Tool\v6\analysis_reports\stats_by_type.csv`
- Stats (Quelle): `C:\Users\Danie\Meine Ablage (moba.apotheken@gmail.com)\00000000000000_UNTERNEHMENSGRUPPE\12_KOLUMBUS_BioComputing_GmbH\001_TDM_Tool\v6\analysis_reports\stats_by_source.csv`
- Missing-Graph (DOT): `C:\Users\Danie\Meine Ablage (moba.apotheken@gmail.com)\00000000000000_UNTERNEHMENSGRUPPE\12_KOLUMBUS_BioComputing_GmbH\001_TDM_Tool\v6\analysis_reports\missing_graph.dot`
- Missing-Graph (PNG): `C:\Users\Danie\Meine Ablage (moba.apotheken@gmail.com)\00000000000000_UNTERNEHMENSGRUPPE\12_KOLUMBUS_BioComputing_GmbH\001_TDM_Tool\v6\analysis_reports\missing_graph.png`