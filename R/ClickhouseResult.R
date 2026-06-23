#' Class ClickhouseResult
#'
#' Encapsulates the result of a statement. Because both transports return the
#' full Arrow result eagerly, a `ClickhouseResult` holds the materialised
#' data.frame and a cursor; [DBI::dbFetch()] slices from it.
#'
#' @export
#' @keywords internal
setClass("ClickhouseResult",
  contains = "DBIResult",
  slots = list(
    sql          = "character",
    conn         = "ClickhouseConnection",
    state        = "environment"
  )
)

# Build a result object. `state` carries mutable cursor/bookkeeping.
#' @noRd
new_clickhouse_result <- function(conn, statement, data = NULL,
                                  is_statement = FALSE, rows_affected = NA_real_) {
  state <- new.env(parent = emptyenv())
  state$data          <- data
  state$is_statement  <- is_statement
  state$rows_affected <- rows_affected
  state$cursor        <- 0L
  state$valid         <- TRUE
  new("ClickhouseResult", sql = statement, conn = conn, state = state)
}

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbFetch", "ClickhouseResult", function(res, n = -1, ...) {
  if (length(n) != 1) stop("n must be a scalar.")
  if (is.infinite(n)) n <- -1
  if (n != as.integer(n) || (n < 0 && n != -1)) {
    stop("n must be a non-negative integer, -1 or Inf.")
  }
  data <- res@state$data
  if (is.null(data)) return(data.frame())

  total <- nrow(data)
  start <- res@state$cursor
  end   <- if (n == -1) total else min(start + n, total)
  rows  <- if (end > start) seq.int(start + 1L, end) else integer(0)
  res@state$cursor <- end
  data[rows, , drop = FALSE]
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbClearResult", "ClickhouseResult", function(res, ...) {
  if (!res@state$valid) warning("Result has already been cleared.")
  res@state$valid <- FALSE
  res@state$data  <- NULL
  invisible(TRUE)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbHasCompleted", "ClickhouseResult", function(res, ...) {
  if (res@state$is_statement) return(TRUE)
  data <- res@state$data
  is.null(data) || res@state$cursor >= nrow(data)
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbGetStatement", "ClickhouseResult", function(res, ...) res@sql)

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbIsValid", "ClickhouseResult", function(dbObj, ...) dbObj@state$valid)

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbGetRowCount", "ClickhouseResult", function(res, ...) res@state$cursor)

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbGetRowsAffected", "ClickhouseResult", function(res, ...) {
  res@state$rows_affected
})

#' @rdname ClickhouseResult-class
#' @export
setMethod("dbColumnInfo", "ClickhouseResult", function(res, ...) {
  data <- res@state$data
  if (is.null(data)) data <- data.frame()
  data.frame(
    name = names(data),
    type = vapply(data, function(x) class(x)[[1]], character(1)),
    row.names = NULL,
    stringsAsFactors = FALSE
  )
})
