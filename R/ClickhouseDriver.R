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
#' clckhs::clickhouse()
clickhouse <- function() {
  new("ClickhouseDriver")
}

#' @rdname ClickhouseDriver-class
#' @export
setMethod("show", "ClickhouseDriver", function(object) {
  cat("<ClickhouseDriver>\n")
})

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

#' Connect to a ClickHouse database.
#' @export
#' @rdname ClickhouseDriver-class
#' @param drv ClickHouse database driver.
#' @param host name of the host on which the database is running.
#' @param port port on which the database is listening.
#' @param db name of the default database.
#' @param user name of the user to connect as.
#' @param password the user's password.
#' @value A database connection.
#' @examples
#' conn <- dbConnect(clckhs::clckhs("localhost", db = "mydb", user = "me")
setMethod("dbConnect", "ClickhouseDriver", function(drv, host, port = 9000, db = "default", user = "default", password = "", ...) {
  new("ClickhouseConnection", ptr = clckhs::connect(host, port, db, user, password))
})
