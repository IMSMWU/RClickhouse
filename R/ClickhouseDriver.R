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


complementList <- function(config, config_temp) {
  diff_params <- checkParameters(config, config_temp)
  for (param in diff_params) {
    param_temp <- c(param_name=config_temp[[param]])
    names(param_temp) <- param
    config <- c(config, param_temp)
  }
  return(config)
}


checkParameters <- function(add_params, comp_params) {
  diff_params = c()
  for (name in names(comp_params)) {
    if (exists(name, where=as.list(add_params)) == FALSE) {
      diff_params <- c(diff_params, name)
    }
  }
  return(diff_params)
}

# helper-function for config-parser
removequotes <- function(parsedline) {
  if (grepl('^["](.*["])?$', parsedline)) {
    return(gsub('^["](.*["])?$', substr(parsedline, 2, nchar(parsedline) -1), parsedline))
  } else if (grepl("^['](.*['])?$", parsedline)) {
    return(gsub("^['](.*['])?$", substr(parsedline, 2, nchar(parsedline) -1), parsedline))
  }
  return(parsedline)
}

configparser <- function(filepath) {
  con <- file(filepath, "r")
  parsedConfig <- list()
  while ( TRUE ) {
    line <- readLines(con, n <- 1)
    if ( length(line) == 0 ) {
      break
    } else if ( nchar(line) == 0 ) {
      next
    }
    splitUp <- strsplit(line, ":", fixed = TRUE)
    unlistedLine <- unlist(splitUp)
    key <- removequotes(trimws(unlistedLine[1]))
    value <- removequotes(trimws(paste(unlistedLine[-1], collapse=":")))
    if ('true' == value || 'TRUE' == value) {
      value <- TRUE
    }else if ('false' == value || 'FALSE' == value) {
      value <- FALSE
    }else if (!is.na(strtoi(value))) {
      value <- strtoi(value)
    }
    parsedConfig[[key]] <- value
  }
  close(con)
  return(parsedConfig)
}

#' @rdname ClickhouseDriver-class
#' @export
#' @param CONFIG_PATHS a list of configuration paths
#' @param DEFAULT_PARAMS a list of configuration defaults
#' @param pre_config initialization set
#' @return a merged configuration
loadConfig <- function(CONFIG_PATHS, DEFAULT_PARAMS, pre_config) {
  config = pre_config
  config_found <- FALSE

  for (path in CONFIG_PATHS) {
    if (file.exists(path) == TRUE) {
      config_temp <- configparser(path)
      config <- complementList(config, config_temp)
      config_found <- TRUE
    }
  }

  if (config_found == TRUE) {
    warning('We have found a config file/multiple config files that will be loaded.')
  }

  if (all(names(DEFAULT_PARAMS) %in% names(config)) == FALSE){
    config <- complementList(config, DEFAULT_PARAMS)
  }
  return(config)
}


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
#' @param config_paths paths where config files are searched for; order of paths denotes hierarchy (first string has highest priority etc.).
#' @param Int64 The R type that 64-bit integer types should be mapped to,
#'   default is [bit64::integer64], which allows the full range of 64 bit
#'   integers.
#' @param toUTF8 logical, should character variables be converted to UTF-8. Default is TRUE.
#' @return A database connection.
#' @examples
#' \dontrun{
#' conn <- dbConnect(RClickhouse::clickhouse(), host="localhost")
#' }
setMethod("dbConnect", "ClickhouseDriver",
          function(drv, host="localhost", port = 9000, dbname = "default",
                   user = "default", password = "", compression = "lz4",
                   config_paths = c('./RClickhouse.yaml', '~/.R/RClickhouse.yaml', '/etc/RClickhouse.yaml'),
                   Int64 = c("integer64", "integer", "numeric", "character"), toUTF8 = TRUE, ...) {
    db <- match.call(expand.dots = TRUE)
    if("db" %in% names(db)){
        warning("Parameter 'db' is deprecated and will be removed in the future. Use 'dbname' instead.")
        dbname <- db$db
    }
            DEFAULT_PARAMS <- c(host='localhost', port=9000, db='default', user='default', password='', compression='lz4')
            input_params <- c(host=host, port=port, db=dbname, user=user, password=password, compression=compression)
            default_input_diff <- c(input_params[!(input_params %in% DEFAULT_PARAMS)])

            config <- loadConfig(config_paths, DEFAULT_PARAMS, default_input_diff)

            Int64 <- match.arg(Int64)

            ptr <- connect(config[['host']], strtoi(config[['port']]), config[['db']], config[['user']], config[['password']], config[['compression']])
            reg.finalizer(ptr, function(p) {
              if (validPtr(p))
                warning("connection was garbage collected without being disconnected")
            })
            new("ClickhouseConnection", ptr = ptr, port = port, host = host, user = user, Int64 = Int64, toUTF8 = toUTF8)
          })

buildEnumType <- function(obj) {
  lvls <- levels(obj)
  idmap <- mapply(function(name, id) paste0(quoteString(name), "=", id), lvls, seq(0, length(lvls)-1))
  return(paste0("Enum16(", paste(idmap, collapse=",", sep=","), ")"))
}

#' @export
#' @importFrom bit64 is.integer64
#' @rdname ClickhouseDriver-class
setMethod("dbDataType", signature(dbObj="ClickhouseDriver", obj = "ANY"), definition = function(dbObj, obj, ...) {
  if (is.null(obj) || all(is.na(obj))) {
    stop("Invalid argument to dbDataType: must not be NULL or all NA")
  }

  if (is.list(obj)) {
    t <- paste0("Array(", dbDataType(dbObj, unlist(obj, recursive=F)), ")")
  } else {
    if (is.factor(obj)) t <- buildEnumType(obj)
    else if (is.integer64(obj)) t <- "Int64"
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
