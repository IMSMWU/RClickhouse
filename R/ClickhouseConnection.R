#' Class ClickhouseConnection
#'
#' `ClickhouseConnection` objects are created by [DBI::dbConnect()] and wrap a
#' transport (Arrow Flight SQL or HTTP). All DBI methods delegate to the
#' transport seam (see `transport.R`); the class exists so that dbplyr can
#' dispatch ClickHouse-specific SQL on it.
#'
#' @export
#' @keywords internal
#' @import methods DBI
setClass("ClickhouseConnection",
  contains = "DBIConnection",
  slots = list(
    transport      = "ANY",        # ch_transport object
    transport_name = "character",
    host           = "character",
    port           = "numeric",
    dbname         = "character",
    user           = "character"
  )
)

#' @export
#' @rdname ClickhouseConnection-class
setMethod("show", "ClickhouseConnection", function(object) {
  cat("<ClickhouseConnection>\n")
  if (dbIsValid(object)) {
    cat("  ", object@user, "@", object@host, ":", object@port,
        "/", object@dbname, " [", object@transport_name, "]\n", sep = "")
  } else {
    cat("  DISCONNECTED\n")
  }
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbGetInfo", "ClickhouseConnection", function(dbObj, ...) {
  env <- dbGetQuery(dbObj, paste(
    "SELECT version() AS version, uptime() AS uptime,",
    "currentDatabase() AS database"))
  list(
    name       = "ClickhouseConnection",
    db.version = env$version,
    uptime     = env$uptime,
    dbname     = env$database,
    username   = dbObj@user,
    host       = dbObj@host,
    port       = dbObj@port,
    transport  = dbObj@transport_name
  )
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbIsValid", "ClickhouseConnection", function(dbObj, ...) {
  ch_valid(dbObj@transport)
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbDisconnect", "ClickhouseConnection", function(conn, ...) {
  ch_close(conn@transport)
  invisible(TRUE)
})

# Query / statement execution -------------------------------------------------

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbGetQuery", signature("ClickhouseConnection", "character"),
  function(conn, statement, ...) {
    ch_query(conn@transport, statement)
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbSendQuery", signature("ClickhouseConnection", "character"),
  function(conn, statement, ...) {
    data <- ch_query(conn@transport, statement)
    new_clickhouse_result(conn, statement, data = data, is_statement = FALSE)
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbSendStatement", signature("ClickhouseConnection", "character"),
  function(conn, statement, ...) {
    affected <- ch_execute(conn@transport, statement)
    new_clickhouse_result(conn, statement, data = NULL,
                          is_statement = TRUE, rows_affected = affected)
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbExecute", signature("ClickhouseConnection", "character"),
  function(conn, statement, ...) {
    affected <- ch_execute(conn@transport, statement)
    if (is.na(affected)) 0 else affected
  })

# Metadata --------------------------------------------------------------------

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbListTables", "ClickhouseConnection", function(conn, ...) {
  as.character(dbGetQuery(conn, "SHOW TABLES")[[1]])
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbExistsTable", c("ClickhouseConnection", "character"),
  function(conn, name, ...) {
    qname <- dbQuoteIdentifier(conn, name)
    as.logical(qname %in% dbQuoteIdentifier(conn, dbListTables(conn)))
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbListFields", c("ClickhouseConnection", "character"),
  function(conn, name, ...) {
    qname <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste("DESCRIBE TABLE", qname))$name
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbReadTable", c("ClickhouseConnection", "character"),
  function(conn, name, ...) {
    qname <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste("SELECT * FROM", qname))
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbRemoveTable", c("ClickhouseConnection", "character"),
  function(conn, name, ...) {
    qname <- dbQuoteIdentifier(conn, name)
    dbExecute(conn, paste("DROP TABLE", qname))
    invisible(TRUE)
  })

# Table creation & writing ----------------------------------------------------

#' @export
#' @rdname ClickhouseConnection-class
#' @param engine the ClickHouse table engine to use when creating a table.
setMethod("dbCreateTable", "ClickhouseConnection",
  function(conn, name, fields, ..., engine = "MergeTree() ORDER BY tuple()",
           row.names = NULL, temporary = FALSE) {
    stopifnot(is.null(row.names), is.logical(temporary), length(temporary) == 1L)
    query <- sqlCreateTable(conn, table = name, fields = fields,
                            row.names = row.names, temporary = temporary)
    query <- paste(query, "ENGINE =", engine)
    dbExecute(conn, query)
    invisible(TRUE)
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbAppendTable", "ClickhouseConnection",
  function(conn, name, value, ..., row.names = NULL) {
    value <- coerce_to_df(value, row.names)
    qname <- dbQuoteIdentifier(conn, name)
    if (!dbExistsTable(conn, name)) {
      stop("Table ", qname, " does not exist. Use dbCreateTable() first.")
    }
    if (nrow(value)) ch_insert(conn@transport, name, value)
    invisible(nrow(value))
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbWriteTable", signature("ClickhouseConnection", "character", "ANY"),
  function(conn, name, value, overwrite = FALSE, append = FALSE,
           engine = "MergeTree() ORDER BY tuple()", row.names = NA,
           field.types = NULL, ...) {
    if (overwrite && append) stop("overwrite and append cannot both be TRUE.")
    value <- coerce_to_df(value, row.names)
    if (!is.null(field.types) &&
        (length(field.types) != length(value) || !is.character(field.types))) {
      stop("field.types must be a character vector with one entry per column.")
    }

    exists <- dbExistsTable(conn, name)
    if (exists && overwrite) {
      dbRemoveTable(conn, name)
      exists <- FALSE
    }
    if (exists && !append) {
      stop("Table ", dbQuoteIdentifier(conn, name), " already exists. ",
           "Set overwrite = TRUE or append = TRUE.")
    }
    if (!exists) {
      if (is.null(field.types)) field.types <- vapply(value, dbDataType,
                                                      character(1), dbObj = conn)
      names(field.types) <- names(value)
      dbCreateTable(conn, name, field.types, engine = engine)
    }
    if (nrow(value)) ch_insert(conn@transport, name, value)
    invisible(TRUE)
  })

# Coerce arbitrary input to a data.frame, optionally adding a row_names column.
#' @noRd
coerce_to_df <- function(value, row.names = NA) {
  if (is.null(row.names)) row.names <- FALSE  # DBI: NULL means "do not add"
  if (is.vector(value) && !is.list(value)) {
    value <- data.frame(x = value, stringsAsFactors = FALSE)
  }
  if (!is.data.frame(value)) value <- as.data.frame(value, stringsAsFactors = FALSE)
  if (length(value) < 1) stop("value must have at least one column.")
  if ((!is.na(row.names) && !is.logical(row.names) && !is.character(row.names)) ||
      length(row.names) != 1) {
    stop("row.names must be NA, logical, or a string.")
  }
  add_col <- NA
  if ((!is.na(row.names) && isTRUE(row.names)) ||
      (is.na(row.names) && .row_names_info(value) >= 0)) {
    add_col <- "row_names"
  } else if (is.character(row.names)) {
    add_col <- row.names
  }
  if (!is.na(add_col)) value[[add_col]] <- as.character(rownames(value))
  rownames(value) <- NULL
  value
}

# Types & quoting -------------------------------------------------------------

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbDataType", signature(dbObj = "ClickhouseConnection", obj = "ANY"),
  function(dbObj, obj, ...) dbDataType(clickhouse(), obj),
  valueClass = "character")

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteIdentifier", c("ClickhouseConnection", "character"),
  function(conn, x, ...) {
    if (anyNA(x)) stop("Input to dbQuoteIdentifier must not contain NA.")
    SQL(quote_ident_backtick(x))
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteIdentifier", c("ClickhouseConnection", "SQL"),
  function(conn, x, ...) x)

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteString", c("ClickhouseConnection", "character"),
  function(conn, x, ...) SQL(quote_string_sql(x)))

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteString", c("ClickhouseConnection", "SQL"),
  function(conn, x, ...) x)

# Transactions (unsupported) --------------------------------------------------

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbBegin", "ClickhouseConnection",
  function(conn, ...) stop("Transactions are not supported by ClickHouse."))

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbCommit", "ClickhouseConnection",
  function(conn, ...) stop("Transactions are not supported by ClickHouse."))

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbRollback", "ClickhouseConnection",
  function(conn, ...) stop("Transactions are not supported by ClickHouse."))
