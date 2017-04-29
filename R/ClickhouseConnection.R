#' Class ClickhouseConnection
#'
#' \code{ClickhouseConnection.} objects are usually created by
#' \code{\link[DBI]{dbConnect}}
#'
#' @export
#' @keywords internal
#' @import methods DBI
setClass("ClickhouseConnection",
  contains = "DBIConnection",
  slots = list(
    ptr  = "externalptr",
    host = "character",
    port = "numeric",
    user = "character"
  )
)

#' @export
#' @rdname ClickhouseConnection-class
setMethod("show", "ClickhouseConnection", function(object) {
  cat("<ClickhouseConnection>\n")
  if (dbIsValid(object)) {
    cat("  ", object@user, "@", object@host, ":", object@port, "\n", sep="")
  } else {
    cat("  DISCONNECTED\n")
  }
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbGetInfo", "ClickhouseConnection", def=function(dbObj, ...) {
  envdata <- dbGetQuery(dbObj, "SELECT version() as version, uptime() as uptime,
                        currentDatabase() as database")

  list(
    name = "ClickhouseConnection",
    db.version = envdata$version,
    uptime     = envdata$uptime,
    dbname     = envdata$database,
    username   = dbObj@user,
    host       = dbObj@host,
    port       = dbObj@port
  )
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbIsValid", "ClickhouseConnection", function(dbObj, ...) {
  tryCatch({
    dbGetQuery(dbObj, "select 1")
    TRUE
  }, error = function(e) {
    print(e)
    FALSE
  })
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbListTables", "ClickhouseConnection", function(conn, ...) {
  as.character(dbGetQuery(conn, "SHOW TABLES")[[1]])
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbExistsTable", c("ClickhouseConnection", "character"), function(conn, name, ...) {
  qname <- dbQuoteIdentifier(conn, name)
  #NOTE: must match on quoted names, since the name argument may already be quoted
  as.logical(qname %in% dbQuoteIdentifier(conn, dbListTables(conn)))
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbReadTable", c("ClickhouseConnection", "character"), function(conn, name, ...) {
  qname <- dbQuoteIdentifier(conn, name)
  dbGetQuery(conn, paste0("SELECT * FROM ", qname))
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbRemoveTable",c("ClickhouseConnection", "character"), function(conn, name, ...) {
  qname <- dbQuoteIdentifier(conn, name)
  dbExecute(conn, paste0("DROP TABLE ", qname))
  invisible(TRUE)
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbListFields", c("ClickhouseConnection", "character"), function(conn, name, ...) {
  qname <- dbQuoteIdentifier(conn, name)
  dbGetQuery(conn, paste0("DESCRIBE TABLE ", qname))$name
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbSendQuery", c("ClickhouseConnection", "character"), function(conn, statement, ...) {
  res <- clckhs::select(conn@ptr, statement);
  return(new("ClickhouseResult",
      sql = statement,
      env = new.env(parent = emptyenv()),   #TODO: set env
      conn = conn,
      ptr = res
  ))
})

setMethod("dbWriteTable", signature(conn = "ClickhouseConnection", name = "character", value = "ANY"), definition = function(conn, name, value, overwrite=FALSE,
                                                                                                                             append=FALSE, engine="TinyLog", ...) {
  if (is.vector(value) && !is.list(value)) value <- data.frame(x = value, stringsAsFactors = F)
  if (length(value) < 1) stop("value must have at least one column")
  if (is.null(names(value))) names(value) <- paste("V", 1:length(value), sep='')
  if (length(value[[1]])>0) {
    if (!is.data.frame(value)) value <- as.data.frame(value, row.names=1:length(value[[1]]) , stringsAsFactors=F)
  } else {
    if (!is.data.frame(value)) value <- as.data.frame(value, stringsAsFactors=F)
  }
  if (overwrite && append) {
    stop("Setting both overwrite and append to TRUE makes no sense.")
  }

  qname <- dbQuoteIdentifier(conn, name)

  if (dbExistsTable(conn, qname)) {
    if (overwrite) dbRemoveTable(conn, qname)
    if (!overwrite && !append) stop("Table ", qname, " already exists. Set overwrite=TRUE if you want
                                    to remove the existing table. Set append=TRUE if you would like to add the new data to the
                                    existing table.")
  }

  if (!dbExistsTable(conn, qname)) {
    fts <- sapply(value, dbDataType, dbObj=conn)
    fdef <- paste(names(value), fts, collapse=', ')
    ct <- paste0("CREATE TABLE ", qname, " (", fdef, ") ENGINE=", engine)
    dbExecute(conn, ct)
  }
  if (length(value[[1]])) {
    classes <- unlist(lapply(value, function(v){
      class(v)[[1]]
    }))
    for (c in names(classes[classes=="character"])) {
      value[[c]] <- enc2utf8(value[[c]])
    }
    for (c in names(classes[classes=="factor"])) {
      levels(value[[c]]) <- enc2utf8(levels(value[[c]]))
    }

    clckhs::insert(conn@ptr, qname, value);
  }

  return(invisible(TRUE))
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbDataType", signature(dbObj="ClickhouseConnection", obj = "ANY"), definition = function(dbObj, obj, ...) {
  dbDataType(clickhouse(), obj)
}, valueClass = "character")

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteIdentifier", c("ClickhouseConnection", "character"),
  function(conn, x, ...) {
    if (anyNA(x)) {
      stop("Input to dbQuoteIdentifier must not contain NA.")
    } else {
      x <- gsub('\\', '\\\\', x, fixed = TRUE)
      x <- gsub('`', '\\`', x, fixed = TRUE)
      SQL(paste0('`', x, '`'))
    }
  }
)

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteIdentifier", c("ClickhouseConnection", "SQL"),
  function(conn, x, ...) { x })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteString", c("ClickhouseConnection", "character"),
  function(conn, x, ...) {
      x <- gsub('\\', '\\\\', x, fixed = TRUE)
      x <- gsub("'", "\\'", x, fixed = TRUE)
      SQL(ifelse(is.na(x), "NULL", paste0("'", x, "'")))
  }
)

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteString", c("ClickhouseConnection", "SQL"),
  function(conn, x, ...) { x })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbBegin", "ClickhouseConnection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbCommit", "ClickhouseConnection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbRollback", "ClickhouseConnection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

#' Close the database connection
#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbDisconnect", "ClickhouseConnection", function(conn, ...) {
  clckhs::disconnect(conn@ptr)
  invisible(TRUE)
})
