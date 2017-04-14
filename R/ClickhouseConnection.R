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
setMethod("dbGetInfo", "ClickhouseConnection", def=function(dbObj, ...) {
  envdata <- dbGetQuery(dbObj, "SELECT version() as version, uptime() as uptime,
                        currentDatabase() as database")

  list(
    name = "ClickhouseConnection",
    db.version = envdata$version,
    uptime     = envdata$uptime,
    dbname     = envdata$database,
    username   = conn@user,
    host       = conn@host,
    port       = conn@port
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
setMethod("dbExistsTable", "ClickhouseConnection", function(conn, name, ...) {
  as.logical(name %in% dbListTables(conn))
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbReadTable", "ClickhouseConnection", function(conn, name, ...) {
  dbGetQuery(conn, paste0("SELECT * FROM ", name))
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbRemoveTable", "ClickhouseConnection", function(conn, name, ...) {
  dbExecute(conn, paste0("DROP TABLE ", name))
  invisible(TRUE)
})

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbSendQuery", "ClickhouseConnection", function(conn, statement, use = c("memory", "temp"), ...) {
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

  qname <- name

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
    write.table(value, textConnection("value_str", open="w"), sep="\t", row.names=F, col.names=F)
    value_str2 <- paste0(get("value_str"), collapse="\n")

    h <- curl::new_handle()
    curl::handle_setopt(h, copypostfields = value_str2)
    req <- curl::curl_fetch_memory(paste0(conn@url, "?query=",URLencode(paste0("INSERT INTO ", qname, " FORMAT TabSeparated"))), handle = h)
    if (req$status_code != 200) {
      stop("Error writing data to table ", rawToChar(req$content))
    }
  }

  return(invisible(TRUE))
  })

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbDataType", signature(dbObj="ClickhouseConnection", obj = "ANY"), definition = function(dbObj,
                                                                                                    obj, ...) {
  if (is.logical(obj)) "UInt8"
  else if (is.integer(obj)) "Int32"
  else if (is.numeric(obj)) "Float64"
  else "String"
}, valueClass = "character")

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

#' @export
DBI::dbGetQuery
