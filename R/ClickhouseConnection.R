#' Class ClickhouseConnection
#'
#' \code{ClickhouseConnection.} objects are usually created by
#' \code{\link[DBI]{dbConnect}}
#'
#' @export
#' @keywords internal
#' @import methods DBI
#' @importFrom bit64 integer64
setClass("ClickhouseConnection",
  contains = "DBIConnection",
  slots = list(
    ptr  = "externalptr",
    host = "character",
    port = "numeric",
    user = "character",
    Int64 = "character",
    toUTF8 = "logical"
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
  if (!validPtr(dbObj@ptr)) {
    FALSE
  } else {
    tryCatch({
      dbGetQuery(dbObj, "select 1")
      TRUE
    }, error = function(e) {
      print(e)
      FALSE
    })
  }
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
setMethod("dbReadTable", c("ClickhouseConnection", "character"), function(conn, name, row.names = NA, ...) {
  if ((!is.na(row.names) && !is.logical(row.names) && !is.character(row.names)) || length(row.names) != 1) {
    stop("row.names must be NA, logical, or a string")
  }

  qname <- dbQuoteIdentifier(conn, name)
  df <- dbGetQuery(conn, paste0("SELECT * FROM ", qname))

  rownames.col <- NA
  if (!is.na(row.names) && row.names == FALSE) {
    rownames(df) <- c()
  } else if (is.na(row.names) || row.names == TRUE) {
    rownames.col <- "row_names"
  } else if (is.character(row.names)) {
    rownames.col <- row.names
  }

  if (!is.na(rownames.col)) {
    if (rownames.col %in% colnames(df)) {
      rownames(df) <- df[[rownames.col]]
    } else if (!is.na(row.names) && (row.names == TRUE || is.character(row.names))) {
      stop(paste0("attempting to read row names from column ", rownames.col,
                  ", which does not exist in table ", qname))
    }
  }

  return(df)
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
  res <- select(conn@ptr, statement);
  return(new("ClickhouseResult",
      sql = statement,
      env = new.env(parent = emptyenv()),   #TODO: set env
      conn = conn,
      ptr = res,
      Int64 = conn@Int64,
      toUTF8 = conn@toUTF8
  ))
})


rch_dbCreateTable <- function (conn, name, fields, engine="TinyLog", overwrite = FALSE, ..., row.names = NULL, temporary = FALSE){
  # removes table if exists and overWrite is true
  qname <- dbQuoteIdentifier(conn, name)
  if (overwrite && dbExistsTable(conn, qname)) dbRemoveTable(conn, qname)

  # copied from DBI::dbCreateTable
  stopifnot(is.null(row.names))
  stopifnot(is.logical(temporary), length(temporary) == 1L)
  query <- sqlCreateTable(con = conn, table = name, fields = fields,
                          row.names = row.names, temporary = temporary, ...)

  # specifies engine --> makes it compatible with Clickhouse
  query <- paste(query, "ENGINE=", engine)

  dbExecute(conn, query)
  invisible(TRUE)
}
setMethod("dbCreateTable", "ClickhouseConnection", rch_dbCreateTable)


rch_dbAppendTable <- function(conn, name, value, ..., row.names = NULL) {
  if (is.vector(value) && !is.list(value)) value <- data.frame(x = value, stringsAsFactors = F)
  if (length(value) < 1) stop("value must have at least one column")
  if (is.null(names(value))) names(value) <- paste("V", 1:length(value), sep='')
  if (length(value[[1]])>0) {
    if (!is.data.frame(value)) value <- as.data.frame(value, row.names=1:length(value[[1]]) , stringsAsFactors=F)
  } else {
    if (!is.data.frame(value)) value <- as.data.frame(value, stringsAsFactors=F)
  }
  if ((!is.na(row.names) && !is.logical(row.names) && !is.character(row.names)) || length(row.names) != 1) {
    stop("row.names must be NA, logical, or a string")
  }

  qname <- dbQuoteIdentifier(conn, name)
  if (!dbExistsTable(conn, qname)) stop("Table ", qname, " doesn't exist. Use dbCreateTable first.")

  rownames.col <- NA
  if ((!is.na(row.names) && row.names == TRUE) || (is.na(row.names) && .row_names_info(value) >= 0)) {
    rownames.col <- "row_names"
  } else if (is.character(row.names)) {
    rownames.col <- row.names
  }
  if (!is.na(rownames.col)) {
    value[rownames.col] <- as.character(rownames(value))
  }

  if (length(value[[1]])) {
    classes <- unlist(lapply(value, function(v){
      class(v)[[1]]
    }))
    for (c in names(classes[classes=="character"])) {
      value[[c]] <- .Internal(setEncoding(value[[c]], "UTF-8"))
    }
    for (c in names(classes[classes=="factor"])) {
      levels(value[[c]]) <- .Internal(setEncoding(levels(value[[c]]), "UTF-8"))
    }
    names(value) <- sapply(names(value),escapeForInternalUse,forsql=FALSE)
    insert(conn@ptr, qname, value);
  }

  return(invisible(TRUE))
}
setMethod("dbAppendTable", "ClickhouseConnection", rch_dbAppendTable)


setMethod("dbWriteTable", signature(conn = "ClickhouseConnection", name = "character", value = "ANY"), definition = function(conn, name, value, overwrite=FALSE,
         append=FALSE, engine="TinyLog", row.names=NA, field.types=NULL, ...) {
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
  if (!is.null(field.types) && (length(field.types) != length(value) || !is.character(field.types))) {
    stop("field.types, if given, must be a string vector with one entry per data columns")
  }
  if ((!is.na(row.names) && !is.logical(row.names) && !is.character(row.names)) || length(row.names) != 1) {
    stop("row.names must be NA, logical, or a string")
  }

  qname <- dbQuoteIdentifier(conn, name)
  if (dbExistsTable(conn, qname)) {
    if (overwrite) dbRemoveTable(conn, qname)
    if (!overwrite && !append) stop("Table ", qname, " already exists. Set overwrite=TRUE if you want
                                    to remove the existing table. Set append=TRUE if you would like to add the new data to the
                                    existing table.")
  }

  rownames.col <- NA
  if ((!is.na(row.names) && row.names == TRUE) || (is.na(row.names) && .row_names_info(value) >= 0)) {
    rownames.col <- "row_names"
  } else if (is.character(row.names)) {
    rownames.col <- row.names
  }
  if (!is.na(rownames.col)) {
    value[rownames.col] <- as.character(rownames(value))
  }

  if (!dbExistsTable(conn, qname)) {
    if (is.null(field.types)) {
      field.types <- sapply(value, dbDataType, dbObj=conn)
    } else if (!is.na(rownames.col)) {
      field.types <- append(field.types, "String")
    }
    fdef <- paste(sapply(names(value),escapeForInternalUse, forsql=TRUE), field.types, collapse=', ')
    ct <- paste0("CREATE TABLE ", qname, " (", fdef, ") ENGINE=", engine)
    dbExecute(conn, ct)
  } else if (is.null(field.types)) {
    # do it for all in general but paticularly for the ones with all NAs...
    # see how the datatypes are being interpreted by chcpp...
    for (i in seq_along(value)) {
      # print(column)
      # print('')
      if (all(is.na(value[i]))) {
        emptyTable <- dbSendQuery(con, paste0("select * from ", qname, " where 1 != 1"))
        column.data.types <- dbColumnInfo(emptyTable)$data.type
        # print('changing it from')
        # print(class(value[[i]]))
        # print(value[i])
        # print('to...')
        # print(column.data.types[i])
        class(value[[i]]) <- column.data.types[i]
      }
    }
  }

  if (length(value[[1]])) {
    classes <- unlist(lapply(value, function(v){
      class(v)[[1]]
    }))
    for (c in names(classes[classes=="character"])) {
      value[[c]] <- .Internal(setEncoding(value[[c]], "UTF-8"))
    }
    for (c in names(classes[classes=="factor"])) {
      levels(value[[c]]) <- .Internal(setEncoding(levels(value[[c]]), "UTF-8"))
    }
    names(value) <- sapply(names(value),escapeForInternalUse,forsql=FALSE)
    insert(conn@ptr, qname, value);
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

quoteString <- function(x) {
  x <- gsub('\\', '\\\\', x, fixed = TRUE)
  x <- gsub("'", "\\'", x, fixed = TRUE)
  return(SQL(ifelse(is.na(x), "NULL", paste0("'", x, "'"))))
}

# removes escape characters and then adds backticks for internal handling
escapeForInternalUse <- function(identifier, forsql) {
  if (grepl('^["](.*["])?$', identifier)) {
    identifier <- gsub('^["](.*["])?$', substr(identifier, 2, nchar(identifier) -1), identifier)
  } else if (grepl("^[`](.*[`])?$", identifier)) {
    identifier <- gsub("^[`](.*[`])?$", substr(identifier, 2, nchar(identifier) -1), identifier)

  }
  if (forsql) {
    return(paste0('`', identifier, '`'))
  }
  else{
    return(identifier)
  }
}

#' @export
#' @rdname ClickhouseConnection-class
setMethod("dbQuoteString", c("ClickhouseConnection", "character"),
  function(conn, x, ...) { quoteString(x) }
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
  if (!validPtr(conn@ptr)) {
    warning("Connection is already disconnected.")
  } else {
    disconnect(conn@ptr)
  }
  invisible(TRUE)
})

