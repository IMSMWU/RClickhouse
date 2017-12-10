
#' @rdname sql_variant
#' adapted from dbplyr
sql_aggregate <- function(f) {
  function(x, na.rm = FALSE) {
    check_na_rm(f, na.rm)
    build_sql(sql(f), list(x))
  }
}

#' @rdname sql_variant
#' adapted from dbplyr
sql_aggregate_2 <- function(f) {
  function(x, y) {
    build_sql(sql(f), list(x, y))
  }
}
