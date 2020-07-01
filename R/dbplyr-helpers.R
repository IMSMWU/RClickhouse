
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

#' Return to dbplyrs original behavior
#'
#' Since Clickhouse functions are case sensitive dbplyr
#' behavior was altered to reflect that. This function
#' will set dbplyr back to its original state of converting
#' unknown functions to upper case.
#' @export
#' @rdname fix_dbplyr
fix_dbplyr <- function(){
  utils::assignInNamespace("sql_prefix", origSQLprefix,
                           ns = "dbplyr", envir = dbpenv)
}

#' Get dbplyr to work with Clickhouse
#'
#' Functions passed to Clickhouse are case sensitive. By default dbplyr
#' converts functions that are not predefined to upper case. This function
#' changes that behavior to leave passed functions as they are.
#' @export
#' @rdname dbplyr_case_sensitive
dbplyr_case_sensitive <- function(){
  base::unlockBinding("sql_prefix", dbpenv)
  utils::assignInNamespace("sql_prefix", ch_sql_prefix,
                           ns = "dbplyr", envir = dbpenv)
  assign("sql_prefix", ch_sql_prefix, envir = dbpenv)
  base::lockBinding("sql_prefix", dbpenv)
}
