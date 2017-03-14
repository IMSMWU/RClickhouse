#' @import methods
#' @importFrom dplyr src_sql
#' @importFrom DBI dbConnect
#' @export
src_clickhouse <- function(dbname = "default", host = "localhost", port = 8123L, user = "default",
                           password = "", ...) {

  con <- DBI::dbConnect(clckhs::clickhouse(), host = host, dbname = dbname,
                        user = user, password = password, port = port, ...)

  dplyr::src_sql("clickhouse", con)
}

#' @export
#' @importFrom dplyr tbl
tbl.src_clickhouse <- function(src, from, ...) {
  dplyr::tbl_sql("clickhouse", src = src, from = from, ...)
}

#' @export
#' @importFrom dplyr src_desc
src_desc.src_clickhouse <- function(con) {
  paste0("clickhouse ","[", con$con@url, "]")
}

db_create_table.src_clickhouse <- function(con, table, types,
                                           temporary = FALSE, engine="TinyLog", ...){

  if (!dbExistsTable(con$con, table)) {
    fts <- sapply(value, dbDataType, dbObj=con$con)
    fdef <- paste(names(value), fts, collapse=', ')
    ct <- paste0("CREATE TABLE ", qname, " (", fdef, ") ENGINE=", engine)
    dbExecute(con$con, ct)
  }
}

#' @importFrom dplyr copy_to
#' @export
#' @return a \code{tbl} object in the remote source
#' @seealso \code{\link[dplyr]{copy_to}}
#' @rdname clckhs
# No transactions supported so
# we have torewrite copy_to
copy_to.src_clickhouse <- function(dest, df, name = deparse(substitute(df)),
                                   overwrite = FALSE, ...) {
  DBI::dbWriteTable(dest$con, name, df, overwrite=overwrite)

  dplyr::tbl(dest, name)
}



#' @export
#' @importFrom dplyr sql_quote
#' @importFrom dplyr sql_escape_string
sql_escape_string.ClickhouseConnection <- function(con, x) {
  dplyr::sql_quote(x, "'")
}

#' @export
#' @importFrom dplyr sql_quote
#' @importFrom dplyr sql_escape_ident
sql_escape_ident.ClickhouseConnection <- function(con, x) {
  dplyr::sql_quote(x, "`")
}

#' @export
#' @importFrom dplyr db_explain
db_explain.ClickhouseConnection <- function(con, sql, ...) {
  stop('clickhouse does not support a plan/explain statement yet.')
}

#' @export
#' @importFrom dplyr db_analyze
db_analyze.ClickhouseConnection <- function(con, sql, ...) {
  # clickhouse does not support a analyze statement.
  TRUE
}

#' @export
#' @importFrom dplyr db_query_fields
db_query_fields.ClickhouseConnection <- function(con, sql, ...) {
  fields <- dplyr::build_sql(
    "SELECT * FROM ", dplyr::sql_subquery(con, sql), " LIMIT 0",
    con = con
  )
  result <- dbSendQuery(con, fields)
  on.exit(dbClearResult(result))
  dbGetInfo(result)$fields$name
}

# SQL translation
#
#' @importFrom dplyr sql_translate_env
#' @export
sql_translate_env.ClickhouseConnection <- function(x) {
  dplyr::sql_variant(
    dplyr::sql_translator(.parent = dplyr::base_scalar,
      "^" = dplyr::sql_prefix("pow"),

      # Casting
      as.logical = dplyr::sql_prefix("toUInt8"),
      as.numeric = dplyr::sql_prefix("toFloat64"),
      as.double = dplyr::sql_prefix("toFloat64"),
      as.integer = dplyr::sql_prefix("toInt64"),
      as.character = dplyr::sql_prefix("toString"),

      # Date/time
      Sys.date = dplyr::sql_prefix("today"),
      Sys.time = dplyr::sql_prefix("now")
    ),
    dplyr::sql_translator(.parent = dplyr::base_agg,
      "%||%" = dplyr::sql_prefix("concat")
    ),
    dplyr::sql_translator(.parent = dplyr::base_win,
      mean  = function(...) stop("window functions are not supported by clickhouse"),
      sum   = function(...) stop("window functions are not supported by clickhouse"),
      min   = function(...) stop("window functions are not supported by clickhouse"),
      max   = function(...) stop("window functions are not supported by clickhouse"),
      n     = function(...) stop("window functions are not supported by clickhouse")
    )
  )
}
