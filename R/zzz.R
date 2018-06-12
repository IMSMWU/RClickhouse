.onUnload <- function(libpath) {
  # Reassign the original 'sql_prefix' function to the dbplyr namespace
  utils::assignInNamespace("sql_prefix", origSQLprefix,
                           ns = "dbplyr", envir = dbpenv)
  gc() # Force garbage collection of connections
}
