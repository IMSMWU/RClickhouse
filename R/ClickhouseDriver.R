#' Class ClickhouseDriver
#'
#' This driver never needs to be unloaded and hence \code{dbUnload()} is a
#' null-op.
#'
#' @export
#' @rdname ClickhouseDriver-class
#' @keywords internal
setClass("ClickhouseDriver",
  contains = "DBIDriver"
)

#' @export
#' @rdname ClickhouseDriver-class
#' @import methods DBI
#' @examples
#' library(DBI)
#' RClickhouse::clickhouse()
clickhouse <- function() {
  new("ClickhouseDriver")
}

#' @rdname ClickhouseDriver-class
#' @export
setMethod("show", "ClickhouseDriver", function(object) {
  cat("<ClickhouseDriver>\n")
})

#' @export
#' @rdname ClickhouseDriver-class
setMethod("dbGetInfo", "ClickhouseDriver", def=function(dbObj, ...) {
  #TODO: return actual version numbers/git hashes
  list(
    driver.version = "alpha",
    client.version = "git"
  )
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
#' @param compression the compression method for the connection (lz4 by default).
#' @return A database connection.
#' @examples
#' \dontrun{
#' conn <- dbConnect(RClickhouse::clickhouse(), host="localhost")
#' }
setMethod("dbConnect", "ClickhouseDriver", function(drv, host="localhost", port = 9000, db = "default", user = "default", password = "", compression = "lz4", ...) {
  ptr <- connect(host, port, db, user, password, compression)
  reg.finalizer(ptr, function(p) {
    if (validPtr(p))
      warning("connection was garbage collected without being disconnected")
  })
  new("ClickhouseConnection", ptr = ptr, port = port, host = host, user = user)
})

buildEnumType <- function(obj) {
  lvls <- levels(obj)
  idmap <- mapply(function(name, id) paste0(quoteString(name), "=", id), lvls, seq(0, length(lvls)-1))
  return(paste0("Enum16(", paste(idmap, collapse=",", sep=","), ")"))
}

#' @export
#' @rdname ClickhouseDriver-class
setMethod("dbDataType", signature(dbObj="ClickhouseDriver", obj = "ANY"), definition = function(dbObj, obj, ...) {
  if (is.null(obj) || all(is.na(obj))) {
    stop("Invalid argument to dbDataType: must not be NULL or all NA")
  }

  if (is.list(obj)) {
    t <- paste0("Array(", dbDataType(dbObj, unlist(obj, recursive=F)), ")")
  } else {
    if (is.factor(obj)) t <- buildEnumType(obj)
    else if (is.logical(obj)) t <- "UInt8"
    else if (is.integer(obj)) t <- "Int32"
    else if (is.numeric(obj)) t <- "Float64"
    else if (inherits(obj, "POSIXct")) t <- "DateTime"
    else if (inherits(obj, "Date")) t <- "Date"
    else t <- "String"

    if (anyNA(obj)) t <- paste0("Nullable(", t, ")")
  }

  return(t)
}, valueClass = "character")
