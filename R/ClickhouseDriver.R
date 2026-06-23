#' Class ClickhouseDriver
#'
#' This driver never needs to be unloaded and hence `dbUnloadDriver()` is a
#' no-op.
#'
#' @export
#' @rdname ClickhouseDriver-class
#' @keywords internal
setClass("ClickhouseDriver", contains = "DBIDriver")

#' Construct a ClickHouse driver object.
#'
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
setMethod("dbGetInfo", "ClickhouseDriver", function(dbObj, ...) {
  list(
    driver.version = as.character(utils::packageVersion("RClickhouse")),
    client.version = as.character(utils::packageVersion("RClickhouse"))
  )
})

#' @rdname ClickhouseDriver-class
#' @export
setMethod("dbIsValid", "ClickhouseDriver", function(dbObj, ...) TRUE)

#' @rdname ClickhouseDriver-class
#' @export
setMethod("dbUnloadDriver", "ClickhouseDriver", function(drv, ...) invisible(TRUE))

# Default port per transport: ClickHouse's Arrow Flight SQL endpoint vs. HTTP.
#' @noRd
default_port <- function(transport, tls = FALSE) {
  switch(transport,
    flightsql = 9090L,
    http      = if (isTRUE(tls)) 8443L else 8123L
  )
}

#' Connect to a ClickHouse database.
#'
#' @export
#' @rdname ClickhouseDriver-class
#' @param drv a [ClickhouseDriver-class] object created by [clickhouse()].
#' @param host host on which the database is running.
#' @param port port to connect to. Defaults to the transport's standard port
#'   (9090 for Flight SQL, 8123/8443 for HTTP).
#' @param dbname name of the default database.
#' @param user name of the user to connect as.
#' @param password the user's password.
#' @param transport communication backend: `"flightsql"` (Arrow Flight SQL via
#'   ADBC, the default) or `"http"` (HTTP interface with the ArrowStream
#'   format). See [RClickhouse-package].
#' @param tls use a TLS-encrypted connection.
#' @param config_paths paths searched for YAML configuration files; earlier
#'   paths take precedence.
#' @param ... passed to the transport. Also accepts deprecated arguments
#'   (`compression`, `Int64`, `toUTF8`, `db`) for backward compatibility.
#' @return A [ClickhouseConnection-class] object.
#' @examples
#' \dontrun{
#' conn <- dbConnect(RClickhouse::clickhouse(), host = "localhost")
#' conn <- dbConnect(RClickhouse::clickhouse(), host = "localhost",
#'                   port = 8123, transport = "http")
#' }
setMethod("dbConnect", "ClickhouseDriver",
  function(drv, host = "localhost", port = NULL, dbname = "default",
           user = "default", password = "",
           transport = c("flightsql", "http"), tls = FALSE,
           config_paths = default_config_paths(), ...) {
    transport <- match.arg(transport)
    dots <- handle_deprecated_connect_args(list(...))
    if (!is.null(dots$dbname)) dbname <- dots$dbname
    dots$dbname <- NULL

    cfg <- ch_resolve_config(
      config_paths,
      list(host = host, port = port, dbname = dbname,
           user = user, password = password,
           transport = transport, tls = tls)
    )
    transport <- match.arg(cfg$transport, c("flightsql", "http"))

    if (is.null(cfg$port)) cfg$port <- default_port(transport, cfg$tls)
    if (identical(as.integer(cfg$port), 9000L)) {
      warning("Port 9000 is the legacy native protocol port, no longer ",
              "supported in RClickhouse 2.0. Use the Arrow Flight SQL port ",
              "(9090) or the HTTP port (8123).", call. = FALSE)
    }

    handle <- do.call(ch_transport_connect, c(
      list(transport = transport, host = cfg$host, port = cfg$port,
           dbname = cfg$dbname, user = cfg$user, password = cfg$password,
           tls = cfg$tls),
      dots
    ))

    new("ClickhouseConnection",
      transport      = handle,
      transport_name = transport,
      host           = cfg$host,
      port           = as.numeric(cfg$port),
      dbname         = cfg$dbname,
      user           = cfg$user
    )
  })

# Map an R vector to the ClickHouse column type used in CREATE TABLE.
#' @noRd
build_enum_type <- function(obj) {
  lvls <- levels(obj)
  idmap <- mapply(function(name, id) paste0(quote_string_sql(name), "=", id),
                  lvls, seq(0, length(lvls) - 1))
  paste0("Enum16(", paste(idmap, collapse = ","), ")")
}

#' @export
#' @importFrom bit64 is.integer64
#' @rdname ClickhouseDriver-class
setMethod("dbDataType", signature(dbObj = "ClickhouseDriver", obj = "ANY"),
  function(dbObj, obj, ...) {
    if (is.null(obj) || all(is.na(obj))) {
      stop("Invalid argument to dbDataType: must not be NULL or all NA")
    }
    if (is.list(obj) && !is.data.frame(obj)) {
      t <- paste0("Array(", dbDataType(dbObj, unlist(obj, recursive = FALSE)), ")")
    } else {
      if (is.factor(obj))            t <- build_enum_type(obj)
      else if (is.integer64(obj))    t <- "Int64"
      else if (is.logical(obj))      t <- "UInt8"
      else if (is.integer(obj))      t <- "Int32"
      else if (is.numeric(obj))      t <- "Float64"
      else if (inherits(obj, "POSIXct")) t <- "DateTime"
      else if (inherits(obj, "Date"))    t <- "Date"
      else                           t <- "String"
      if (anyNA(obj)) t <- paste0("Nullable(", t, ")")
    }
    t
  }, valueClass = "character")
