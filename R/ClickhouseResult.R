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
    conn = "ClickhouseConnection",
    ptr = "externalptr"
  )
)

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbFetch", signature = "ClickhouseResult", definition = function(res, n = -1, ...) {
  return(clckhs::fetch(res@ptr, n))
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbClearResult", "ClickhouseResult", definition = function(res, ...) {
  clckhs::clearResult(res@ptr)
  invisible(TRUE)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbHasCompleted", "ClickhouseResult", definition = function(res, ...) {
  clckhs::hasCompleted(res@ptr)
})

#' @rdname ClickhouseResult-class
#' @inheritParams DBI::dbGetStatement
#' @export
setMethod("dbGetStatement", "ClickhouseResult", function(res, ...) {
  dbIsValid(res)
  res@sql
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbIsValid", "ClickhouseResult", function(dbObj, ...) {
  # todo
  stop("not implemented yet")
})

#' @rdname ClickhouseResult-class
#' @inheritParams DBI::dbGetRowCount
#' @export
setMethod("dbGetRowCount", "ClickhouseResult", function(res, ...) {
  clckhs::getRowCount(res@ptr)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbGetRowsAffected", "ClickhouseResult", definition = function(res, ...) {
  clckhs::getRowsAffected(res@ptr)
})
