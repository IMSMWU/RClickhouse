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
    dplyr::base_no_win
  )
}
