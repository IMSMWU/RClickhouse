
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
