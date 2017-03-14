#' Class ClickhouseResult
#'
#' Clickhouse's query results class.  This classes encapsulates the result of an SQL
#' statement (either \code{select} or not).
#'
#' @export
#' @keywords internal
setClass("ClickhouseResult",
  contains = "DBIResult",
  slots = list(
    sql = "character",
    env = "environment",
    conn = "ClickhouseConnection"
  )
)

#' @rdname ClickhouseResult-class
#' @export
setMethod("fetch", signature(res = "ClickhouseResult", n = "numeric"), definition = function(res, n, ...) {
  if (!dbIsValid(res) || dbHasCompleted(res)) {
    stop("Cannot fetch results from exhausted, closed or invalid response.")
  }
  if (n == 0) {
    stop("Fetch 0 rows? Really?")
  }
  if (res@env$delivered < 0) {
    res@env$delivered <- 0
  }
  if (res@env$delivered >= res@env$rows) {
    return(res@env$data[F,, drop=F])
  }
  if (n > -1) {
    n <- min(n, res@env$rows - res@env$delivered)
    res@env$delivered <- res@env$delivered + n
    return(res@env$data[(res@env$delivered - n + 1):(res@env$delivered),, drop=F])
  }
  else {
    start <- res@env$delivered + 1
    res@env$delivered <- res@env$rows
    return(res@env$data[start:res@env$rows,, drop=F])
  }
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbGetRowsAffected", "ClickhouseResult", definition = function(res, ...) {
  as.numeric(NA)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbClearResult", "ClickhouseResult", definition = function(res, ...) {
  res@env$open <- FALSE
  invisible(TRUE)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbHasCompleted", "ClickhouseResult", definition = function(res, ...) {
  res@env$delivered >= res@env$rows
})


#' @rdname ClickhouseResult-class
#' @export
setMethod("dbIsValid", "ClickhouseResult", function(dbObj, ...) {
  dbObj@env$success && dbObj@env$open
})

#' @rdname ClickhouseResult-class
#' @inheritParams DBI::dbGetStatement
#' @export
setMethod("dbGetStatement", "ClickhouseResult", function(res, ...) {
  dbIsValid(res)
  res@sql
})

#' @rdname ClickhouseResult-class
#' @inheritParams DBI::dbGetRowCount
#' @export
setMethod("dbGetRowCount", "ClickhouseResult", function(res, ...) {
  dbIsValid(res)
  res@env$rows
})
