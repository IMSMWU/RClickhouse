#' @import methods
#' @import assertthat
#' @importFrom dplyr src_sql
#' @importFrom DBI dbConnect
#' @export
src_clickhouse <- function(dbname = "default", host = "localhost", port = 8123L, user = "default",
                           password = "", ...) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("dplyr is required to use src_clickhouse", call. = FALSE)
  }

  con <- DBI::dbConnect(clckhs::Clickhouse(), host = host, dbname = dbname,
                        user = user, password = password, port = port, ...)
  src_dbi(con)
}


#' @export
#' @importFrom dplyr src_desc
src_desc.src_clickhouse <- function(con) {
  paste0("clickhouse ","[", con$con@url, "]")
}

#' @export
sql_escape_string.src_clickhouse <- function(con, x) {
  sql_quote(x, "'")
}

#' @export
sql_escape_ident.src_clickhouse <- function(con, x) {
  sql_quote(x, "`")
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

# No transactions supported so
# we have torewrite copy_to
#importFrom dplyr copy_to
#export
#return a \code{tbl} object in the remote source
#seealso \code{\link[dplyr]{copy_to}}
#copy_to.src_clickhouse <- function(dest, df, name = deparse(substitute(df)),
#                                   overwrite = FALSE, ...) {
#  dbWriteTable(dest$con, name, df, overwrite=overwrite)
#
#  #dplyr:::db_create_indexes(con, name, indexes)
#  #if (analyze)
#  #  db_analyze(con, name)
#
#  tbl(dest, name)
#}


#' @export
#' @importFrom dplyr db_explain
db_explain.clickhouse_connection <- function(con, sql, ...) {
  stop('clickhouse does not support a plan/explain statement yet.')
}

#' @export
#' @importFrom dplyr db_analyze
db_analyze.clickhouse_connection <- function(con, sql, ...) {
  # clickhouse does not support a analyze statement.
  TRUE
}

#' @export
#' @importFrom dplyr db_insert_into
#db_insert_into.clickhouse_connection <- function(con, sql, ...) {
#  stop('TODO. Not implemented yet')
#}
