#' @export
src_clickhouse <- function(dbname = "", host = "localhost", port = 8123L, user = "",
                           password = "", ...) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("dplyr is required to use src_clickhouse", call. = FALSE)
  }

  con <- DBI::dbConnect(clckhs::Clickhouse(), host = host, dbname = dbname,
                        user = user, password = password, port = port, ...)

  #src_sql("clickhouse", con)
  src_dbi(con)
}


# No transactions supported so
# we have torewrite copy_to
#' @export
copy_to.src_clickhouse <- function(dest, df, name = deparse(substitute(df)),
                                   overwrite = FALSE, ...) {
  dbWriteTable(dest$con, name, df, overwrite=overwrite)

  #dplyr:::db_create_indexes(con, name, indexes)
  #if (analyze)
  #  db_analyze(con, name)

  tbl(dest, name)
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
