
#' adapted from dbplyr
#' @rdname sql_variant
#' @param f SQL function name
sql_aggregate <- function(f) {
  function(x, na.rm = FALSE) {
    if (!identical(na.rm, TRUE)) {
      warning("Missing values are always removed in SQL.\n",
              "Use `", f, "(x, na.rm = TRUE)` to silence this warning.", call.=F)
    }
    dbplyr::build_sql(dbplyr::sql(f), list(x))
  }
}

#' adapted from dbplyr
#' @rdname sql_variant
sql_aggregate_2 <- function(f) {
  function(x, y) {
    dbplyr::build_sql(dbplyr::sql(f), list(x, y))
  }
}

#' Since Clickhouse functions are case sensitive dbplyr
#' behavior was altered to reflect that. This function
#' will set dbplyr back to its original state of converting
#' unknown functions to upper case.
#' @rdname fix_dbplyr
fix_dbplyr <- function(){

  utils::assignInNamespace("sql_prefix", curSQLprefix,
                           ns = "dbplyr", envir = dbpenv)
}
