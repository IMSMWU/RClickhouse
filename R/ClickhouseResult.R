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
    ptr = "externalptr",
    bigint = "character"
  )
)

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbFetch", signature = "ClickhouseResult", definition = function(res, n = -1, ...) {
  if (length(n) > 1) stop("n must be integer")
  if (is.infinite(n)) n <- -1
  if (n != as.integer(n) || (n < 0 && n != -1)) {
    stop("n must be a positive integer, -1 or Inf")
  }
  ret <- fetch(res@ptr, n)
  ret <- convert_bigint(ret, res@bigint)
  return(ret)
})

convert_bigint <- function(df, bigint) {
  if (bigint == "integer64") return(df)
  is_int64 <- which(vlapply(df, inherits, "integer64"))
  if (length(is_int64) == 0) return(df)

  as_bigint <- switch(bigint,
                      integer = as.integer,
                      numeric = as.numeric,
                      character = as.character
  )

  df[is_int64] <- suppressWarnings(lapply(df[is_int64], as_bigint))
  df
}


#' @rdname ClickhouseResult-class
#' @export
setMethod("dbClearResult", "ClickhouseResult", definition = function(res, ...) {
  if (!validPtr(res@ptr)) {
    warning("Result has already been cleared.")
  } else {
    clearResult(res@ptr)
  }
  invisible(TRUE)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbHasCompleted", "ClickhouseResult", definition = function(res, ...) {
  hasCompleted(res@ptr)
})

#' @rdname ClickhouseResult-class
#' @inheritParams DBI::dbGetStatement
#' @export
setMethod("dbGetStatement", "ClickhouseResult", function(res, ...) {
  getStatement(res@ptr)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbIsValid", "ClickhouseResult", function(dbObj, ...) {
  validPtr(dbObj@ptr)
})

#' @rdname ClickhouseResult-class
#' @inheritParams DBI::dbGetRowCount
#' @export
setMethod("dbGetRowCount", "ClickhouseResult", function(res, ...) {
  getRowCount(res@ptr)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbGetRowsAffected", "ClickhouseResult", definition = function(res, ...) {
  getRowsAffected(res@ptr)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbColumnInfo", "ClickhouseResult", definition = function(res, ...) {
  df <- dbFetch(res, 0)
  data.frame(
    name = colnames(df),
    field.type = resultTypes(res@ptr),
    data.type = sapply(df, class)
  )
})
