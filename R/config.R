# Connection configuration profiles -------------------------------------------
#
# Connection parameters may be supplied directly to dbConnect() or stored in
# simple "key: value" YAML-style files. Resolution precedence (highest first):
#   1. arguments explicitly set by the caller (i.e. differing from the default)
#   2. values found in configuration files (earlier paths win)
#   3. built-in defaults

#' @noRd
default_config_paths <- function() {
  c("./RClickhouse.yaml", "~/.R/RClickhouse.yaml", "/etc/RClickhouse.yaml")
}

#' @noRd
connect_defaults <- function() {
  list(host = "localhost", port = NULL, dbname = "default", user = "default",
       password = "", transport = "flightsql", tls = FALSE)
}

# Resolve the effective connection parameters.
#' @noRd
ch_resolve_config <- function(config_paths, args) {
  defaults <- connect_defaults()

  # Treat an argument as "explicit" only when it differs from the default.
  explicit <- args[!vapply(names(args), function(nm)
    identical(args[[nm]], defaults[[nm]]), logical(1))]

  file_cfg <- loadConfig(config_paths)

  cfg <- utils::modifyList(defaults, file_cfg)
  cfg <- utils::modifyList(cfg, explicit)
  cfg
}

#' Load ClickHouse connection profiles from configuration files.
#'
#' Reads simple `key: value` files and merges them (earlier paths take
#' precedence). Recognised keys are `host`, `port`, `dbname` (or the legacy
#' `db`), `user`, `password`, `transport` and `tls`.
#'
#' @param config_paths character vector of file paths to search.
#' @return A named list of connection parameters (possibly empty).
#' @export
loadConfig <- function(config_paths = default_config_paths()) {
  out <- list()
  found <- FALSE
  for (path in config_paths) {
    if (file.exists(path)) {
      out <- utils::modifyList(parse_config_file(path), out) # earlier paths win
      found <- TRUE
    }
  }
  if (found && !is.null(out$db) && is.null(out$dbname)) {
    out$dbname <- out$db
  }
  out$db <- NULL
  out
}

# Parse a single "key: value" config file into a typed list.
#' @noRd
parse_config_file <- function(filepath) {
  lines <- readLines(filepath, warn = FALSE)
  cfg <- list()
  for (line in lines) {
    if (!nzchar(trimws(line)) || startsWith(trimws(line), "#")) next
    parts <- strsplit(line, ":", fixed = TRUE)[[1]]
    if (length(parts) < 2) next
    key   <- strip_quotes(trimws(parts[1]))
    value <- strip_quotes(trimws(paste(parts[-1], collapse = ":")))
    cfg[[key]] <- coerce_config_value(value)
  }
  cfg
}

#' @noRd
coerce_config_value <- function(value) {
  if (value %in% c("true", "TRUE"))  return(TRUE)
  if (value %in% c("false", "FALSE")) return(FALSE)
  num <- suppressWarnings(as.integer(value))
  if (!is.na(num) && as.character(num) == value) return(num)
  value
}

#' @noRd
strip_quotes <- function(x) {
  if (grepl('^".*"$', x) || grepl("^'.*'$", x)) substr(x, 2, nchar(x) - 1) else x
}
