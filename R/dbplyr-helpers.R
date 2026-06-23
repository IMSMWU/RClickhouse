# dbplyr SQL-generation helpers ------------------------------------------------

# Package-local state for the optional case-sensitivity patch.
.ch_dbplyr <- new.env(parent = emptyenv())

# Like dbplyr::sql_prefix but does NOT upper-case the function name, since
# ClickHouse function names are case-sensitive.
#' @importFrom dbplyr sql_prefix
ch_sql_prefix <- function(f) {
  function(..., na.rm) {
    dbplyr::build_sql(dbplyr::sql(f), list(...))
  }
}

#' adapted from dbplyr
#' @rdname sql_variant
#' @param f SQL function name
sql_aggregate <- function(f) {
  function(x, na.rm = FALSE) {
    if (!identical(na.rm, TRUE)) {
      warning("Missing values are always removed in SQL.\n",
              "Use `", f, "(x, na.rm = TRUE)` to silence this warning.", call. = FALSE)
    }
    dbplyr::build_sql(dbplyr::sql(f), list(x))
  }
}

#' adapted from dbplyr
#' @rdname sql_variant
sql_aggregate_2 <- function(f) {
  function(x, y) dbplyr::build_sql(dbplyr::sql(f), list(x, y))
}

#' Make dbplyr treat unknown functions as case-sensitive.
#'
#' By default dbplyr upper-cases unknown SQL function names. ClickHouse function
#' names are case-sensitive, so this opt-in helper patches dbplyr to pass them
#' through unchanged. Call [fix_dbplyr()] to restore the default behaviour.
#'
#' @export
#' @rdname dbplyr_case_sensitive
#' @return No return value, called for side effects.
dbplyr_case_sensitive <- function() {
  if (is.null(.ch_dbplyr$original)) {
    .ch_dbplyr$original <- dbplyr::sql_prefix
  }
  dbpenv <- environment(dbplyr::build_sql)
  base::unlockBinding("sql_prefix", dbpenv)
  utils::assignInNamespace("sql_prefix", ch_sql_prefix, ns = "dbplyr", envir = dbpenv)
  assign("sql_prefix", ch_sql_prefix, envir = dbpenv)
  base::lockBinding("sql_prefix", dbpenv)
  invisible(TRUE)
}

#' Restore dbplyr's default function-name handling.
#'
#' Undoes [dbplyr_case_sensitive()].
#'
#' @export
#' @rdname fix_dbplyr
#' @return No return value, called for side effects.
fix_dbplyr <- function() {
  if (is.null(.ch_dbplyr$original)) return(invisible(TRUE))
  dbpenv <- environment(dbplyr::build_sql)
  base::unlockBinding("sql_prefix", dbpenv)
  utils::assignInNamespace("sql_prefix", .ch_dbplyr$original, ns = "dbplyr", envir = dbpenv)
  assign("sql_prefix", .ch_dbplyr$original, envir = dbpenv)
  base::lockBinding("sql_prefix", dbpenv)
  invisible(TRUE)
}
