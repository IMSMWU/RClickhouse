
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

#' adapted from dbplyr
#' @rdname sql_variant
sql_infix <- function(f) {
  # TODO should we add assertthat and rlang to depends?
  # assert_that(is_string(f))
  stopifnot(is.character(f)) # base version, less verbose in errors
  function(x, y) {
    dbplyr::build_sql(x, " ", dbplyr::sql(f), " ", y)
  }
}

#' adapted from dbplyr
#' @rdname sql_variant
sql_prefix <- function(f, n = NULL) {
  # TODO should we add assertthat and rlang to depends?
  # assert_that(is_string(f))
  stopifnot(is.character(f))
  function(...) {
    args <- list(...)
    if (!is.null(n) && length(args) != n) {
      stop(
        "Invalid number of args to SQL ", f, ". Expecting ", n,
        call. = FALSE
      )
    }
    if (any(names2(args) != "")) {
      warning("Named arguments ignored for SQL ", f, call. = FALSE)
    }
    dbplyr::build_sql(dbplyr::sql(f), args)
  }
}