# dbplyr backend --------------------------------------------------------------

#' @export
#' @importFrom dbplyr dbplyr_edition
dbplyr_edition.ClickhouseConnection <- function(con) 2L

#' @export
#' @importFrom dbplyr db_connection_describe
db_connection_describe.ClickhouseConnection <- function(con, ...) {
  info <- dbGetInfo(con)
  uptime_days <- round(info$uptime / 60 / 60 / 24, digits = 2)
  paste0("clickhouse ", info$db.version, " [", info$username, "@",
         info$host, ":", info$port, "/", info$dbname,
         "; uptime: ", uptime_days, " days]")
}

#' @export
#' @importFrom dbplyr sql_table_analyze
sql_table_analyze.ClickhouseConnection <- function(con, table, ...) {
  # ClickHouse has no ANALYZE statement.
  TRUE
}

#' @export
#' @importFrom dbplyr sql_translation
sql_translation.ClickhouseConnection <- function(con) {
  dbplyr::sql_variant(
    dbplyr::sql_translator(.parent = dbplyr::base_scalar,
      `^`          = ch_sql_prefix("pow"),
      # Casting
      as.logical   = ch_sql_prefix("toUInt8"),
      as.numeric   = ch_sql_prefix("toFloat64"),
      as.double    = ch_sql_prefix("toFloat64"),
      as.integer   = ch_sql_prefix("toInt64"),
      as.character = ch_sql_prefix("toString"),
      # Comparison
      is.null      = ch_sql_prefix("isNull"),
      is.na        = ch_sql_prefix("isNull"),
      # Date/time
      Sys.date     = ch_sql_prefix("today"),
      Sys.time     = ch_sql_prefix("now")
    ),
    dbplyr::sql_translator(.parent = dbplyr::base_agg,
      var = ch_sql_prefix("varSamp"),
      sd  = ch_sql_prefix("stddevSamp")
    ),
    dbplyr::base_no_win
  )
}

#' @export
#' @importFrom dbplyr sql_escape_logical
sql_escape_logical.ClickhouseConnection <- function(con, x) {
  if (is.na(x)) "NULL" else as.character(as.integer(x))
}

#' @export
#' @importFrom dbplyr db_copy_to
db_copy_to.ClickhouseConnection <- function(con, table, values,
                                            overwrite = FALSE, types = NULL,
                                            temporary = TRUE,
                                            unique_indexes = NULL, indexes = NULL,
                                            analyze = FALSE, all_nullable = FALSE,
                                            ...) {
  if (isTRUE(analyze))        warning("ClickHouse does not support ANALYZE.")
  if (!is.null(unique_indexes)) warning("ClickHouse does not support unique indexes.")
  if (!is.null(indexes))      warning("ClickHouse does not support indexes.")

  if (is.null(types)) types <- vapply(values, dbDataType, character(1), dbObj = con)
  names(types) <- names(values)

  if (isTRUE(all_nullable)) {
    to_wrap <- !grepl("Nullable", types)
    types[to_wrap] <- paste0("Nullable(", types[to_wrap], ")")
  }

  if (overwrite && dbExistsTable(con, table)) dbRemoveTable(con, table)
  dbCreateTable(con, table, types)
  dbAppendTable(con, table, values)
  table
}
