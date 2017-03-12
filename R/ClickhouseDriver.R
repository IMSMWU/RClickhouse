#' Class ClickhouseDriver
#'
#' This driver never needs to be unloaded and hence \code{dbUnload()} is a
#' null-op.
#'
#' @export
#' @keywords internal
setClass("ClickhouseDriver",
  contains = "DBIDriver"
)

#' @export
#' @import methods DBI
#' @examples
#' library(DBI)
#' clckhs::Clickhouse()
Clickhouse <- function() {
  new("ClickhouseDriver")
}

#' @rdname ClickhouseDriver-class
#' @export
setMethod("dbIsValid", "ClickhouseDriver", function(dbObj, ...) {
  TRUE
})

#' @rdname ClickhouseDriver-class
#' @export
setMethod("dbUnloadDriver", "ClickhouseDriver", function(drv, ...) {
  invisible(TRUE)
})

#' @rdname ClickhouseDriver-class
#' @export
setMethod("dbConnect", "ClickhouseDriver",
  function(drv, host="localhost", port=8123L, user="default", password="", ...) {
    con <- new("ClickhouseConnection",
       url = paste0("http://", user, ":", password, "@", host, ":", port, "/")
    )
    stopifnot(dbIsValid(con))
    con
  }
)
