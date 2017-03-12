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
    url = "character"
  )
)

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbGetInfo", "ClickhouseConnection", def=function(dbObj, ...) {
  envdata <- dbGetQuery(dbObj, "SELECT version() as version, uptime() as uptime,
                        currentDatabase() as database")

  ll$name <- "ClickhouseConnection"
  ll$db.version <- envdata$version
  ll$uptime <- envdata$uptime
  ll$url <- dbObj@url
  ll$dbname <- envdata$database
  ll$username <- NA
  ll$host <- NA
  ll$port <- NA

  ll
})


setMethod("dbIsValid", "ClickhouseConnection", function(dbObj, ...) {
  tryCatch({
    dbGetQuery(dbObj, "select 1")
    TRUE
  }, error = function(e) {
    print(e)
    FALSE
  })
})

clickhouse <- function() {
  new("ClickhouseDriver")
}



setMethod("dbListTables", "ClickhouseConnection", function(conn, ...) {
  as.character(dbGetQuery(conn, "SHOW TABLES")[[1]])
})

setMethod("dbExistsTable", "ClickhouseConnection", function(conn, name, ...) {
  as.logical(name %in% dbListTables(conn))
})

setMethod("dbReadTable", "ClickhouseConnection", function(conn, name, ...) {
  dbGetQuery(conn, paste0("SELECT * FROM ", name))
})

setMethod("dbRemoveTable", "ClickhouseConnection", function(conn, name, ...) {
  dbExecute(conn, paste0("DROP TABLE ", name))
  invisible(TRUE)
})

#' @importFrom data.table fread
setMethod("dbSendQuery", "ClickhouseConnection", function(conn, statement, use = c("memory", "temp"), ...) {
  # with use = "temp" we try to avoid exception with long vectors conversion in rawToChar
  # <simpleError in rawToChar(req$content): long vectors are not supported yet: raw.c:68>
  use <- match.arg(use)

  q <- sub("[; ]*;\\s*$", "", statement, ignore.case=T, perl=T)
  has_resultset <- grepl("^\\s*(SELECT|SHOW)\\s+", statement, perl=T, ignore.case=T)

  if (has_resultset) {
    if ( grepl(".*FORMAT\\s+\\w+\\s*$", statement, perl=T, ignore.case=T)) {
      stop("Can't have FORMAT keyword in queries, query ", statement)
    }
    q <- paste0(q ," FORMAT TabSeparatedWithNames")
  }

  h <- curl::new_handle()
  curl::handle_setopt(h, copypostfields = q)

  if (use == "memory") {
    req <- curl::curl_fetch_memory(conn@url, handle = h)
  } else {
    tmp <- tempfile()
    req <- curl::curl_fetch_disk(conn@url, tmp, handle = h)
  }

  if (req$status_code != 200) {
    if (use == "memory") {
      stop(rawToChar(req$content))
    } else {
      stop(readLines(tmp))
    }
  }

  dataenv <- new.env(parent = emptyenv())
  if (has_resultset) {
    # try to avoid problems when select just one column that can contain ""
    # without "blank.lines.skip" we'll get warning:
    # Stopped reading at empty line ... but text exists afterwards (discarded): ...
    # and not all rows will be read
    if (use == "memory") {
      dataenv$data <- data.table::fread(rawToChar(req$content), sep="\t", header=TRUE,
                                        showProgress=FALSE,
                                        blank.lines.skip = TRUE)
    } else {
      dataenv$data <- data.table::fread(tmp, sep = "\t", header = TRUE,
                                        showProgress = FALSE,
                                        blank.lines.skip = TRUE)
      unlink(tmp)
    }
  }
  dataenv$success <- TRUE

  dataenv$delivered <- -1
  dataenv$open <- TRUE
  dataenv$rows <- nrow(dataenv$data)

  new("ClickhouseResult",
      sql = statement,
      env = dataenv,
      conn = conn
  )
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

setMethod("dbDataType", signature(dbObj="ClickhouseConnection", obj = "ANY"), definition = function(dbObj,
                                                                                                    obj, ...) {
  if (is.logical(obj)) "UInt8"
  else if (is.integer(obj)) "Int32"
  else if (is.numeric(obj)) "Float64"
  else "String"
}, valueClass = "character")

setMethod("dbBegin", "ClickhouseConnection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

setMethod("dbCommit", "ClickhouseConnection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

setMethod("dbRollback", "ClickhouseConnection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

setMethod("dbDisconnect", "ClickhouseConnection", function(conn, ...) {
  invisible(TRUE)
})
