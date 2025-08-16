# parser.R
# Dieses Skript wandelt eine R-Datei in eine *typisierte* JSON-Repräsentation ihres AST um.
# Es wird vom Python-Hauptskript aufgerufen.

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Benötigt das Paket 'jsonlite'. Bitte installieren Sie es mit install.packages('jsonlite')", call. = FALSE)
}
library(jsonlite)

# Rekursive Funktion: bildet R-Expressions auf ein gut konsumierbares JSON ab
expr_to_obj <- function(e) {
  if (is.call(e)) {
    fn <- as.character(e[[1]])
    args <- as.list(e)[-1]
    nm <- names(args)
    args_json <- lapply(args, expr_to_obj)
    if (!is.null(nm)) names(args_json) <- nm
    list(type = "call", fun = fn, args = args_json)
  } else if (is.name(e)) {
    list(type = "name", value = as.character(e))
  } else if (is.atomic(e)) {
    # Achtung: atomare Vektoren können länger als 1 sein
    list(type = "literal", value = e)
  } else {
    list(type = class(e)[1], value = as.character(e))
  }
}

# CLI-Argumente: <input_file> <output_file>
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2) {
  stop("Benötigt werden: <input_file> <output_file>", call. = FALSE)
}
input_file <- args[[1]]
output_file <- args[[2]]

# Parsen mit Fehlerbehandlung
parsed <- try(parse(file = input_file), silent = TRUE)

if (inherits(parsed, "try-error")) {
  write_json(list(error = "Parse error in R file", file = basename(input_file)), output_file, auto_unbox = TRUE, pretty = TRUE)
  quit(status = 1)
} else {
  ast <- lapply(parsed, expr_to_obj)
  write_json(ast, output_file, auto_unbox = TRUE, pretty = TRUE)
}
