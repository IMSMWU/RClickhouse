#' @export
#' @importFrom dplyr db_desc
db_desc.ClickhouseConnection <- function(con) {
  info <- dbGetInfo(con)

  uptime_days <- round(info$uptime/60/60/24, digits = 2)

  paste0("clickhouse ", info$db.version, " [", info$username, "@",
         info$host, ":", info$port, "/", info$dbname, "; uptime: ",uptime_days ," days ]")
}

#' @export
#' @importFrom dplyr sql_escape_string
sql_escape_string.ClickhouseConnection <- function(con, x) {
  encodeString(x, na.encode = FALSE, quote = "'")
}

#' @export
#' @importFrom dplyr sql_escape_ident
sql_escape_ident.ClickhouseConnection <- function(con, x) {
  encodeString(x, na.encode = FALSE, quote = "`")
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

# SQL translation
#
#' @importFrom dplyr sql_translate_env
#' @export
sql_translate_env.ClickhouseConnection <- function(x) {
  dbplyr::sql_variant(
    dbplyr::sql_translator(.parent = dbplyr::base_scalar,
      "^" = dbplyr::sql_prefix("pow"),

      # Casting
      as.logical = dbplyr::sql_prefix("toUInt8"),
      as.numeric = dbplyr::sql_prefix("toFloat64"),
      as.double = dbplyr::sql_prefix("toFloat64"),
      as.integer = dbplyr::sql_prefix("toInt64"),
      as.character = dbplyr::sql_prefix("toString"),

      # Date/time
      Sys.date = dbplyr::sql_prefix("today"),
      Sys.time = dbplyr::sql_prefix("now")
    ),
    dbplyr::sql_translator(.parent = dbplyr::base_agg,
      "%||%" = dbplyr::sql_prefix("concat")
    ),
    dbplyr::base_no_win
  )
}
