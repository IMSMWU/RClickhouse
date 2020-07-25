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

# As opposed to the original sql_prefix, do NOT convert the function
# name to upper case (function names are case-sensitive in
# Clickhouse)
#' @importFrom dbplyr sql_prefix
ch_sql_prefix <- function(f) {
  function(..., na.rm) {
    dbplyr::build_sql(dbplyr::sql(f), list(...))
  }
}

# Overwrite sql_prefix
# Adapted from R.utils' "reassignInPackage" function
curSQLprefix <- dbplyr::sql_prefix
if(is.null(attr(ch_sql_prefix, 'original'))){
  attr(ch_sql_prefix, 'original') <- curSQLprefix
}
dbpenv <- environment(dbplyr::build_sql)
base::unlockBinding("sql_prefix", dbpenv)
utils::assignInNamespace("sql_prefix", ch_sql_prefix,
                         ns = "dbplyr", envir = dbpenv)
assign("sql_prefix", ch_sql_prefix, envir = dbpenv)
base::lockBinding("sql_prefix", dbpenv)
origSQLprefix <- attr(dbplyr::sql_prefix, 'original')

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
                           `^` = ch_sql_prefix("pow"),

                           # Casting
                           as.logical = ch_sql_prefix("toUInt8"),
                           as.numeric = ch_sql_prefix("toFloat64"),
                           as.double = ch_sql_prefix("toFloat64"),
                           as.integer = ch_sql_prefix("toInt64"),
                           as.character = ch_sql_prefix("toString"),

                           # Comparison
                           is.null = ch_sql_prefix("isNull"),
                           is.na   = ch_sql_prefix("isNull"),

                           # Date/time
                           Sys.date = ch_sql_prefix("today"),
                           Sys.time = ch_sql_prefix("now")
    ),
    dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      `%||%` = ch_sql_prefix("concat"),
      #cat     = ch_sql_prefix("concat"),
      var     = ch_sql_prefix("varSamp"),
      sd      = ch_sql_prefix("stddevSamp")
    ),
    dbplyr::base_no_win
  )
}

# DBIConnection fork without transactions
#' @importFrom dbplyr db_copy_to
#' @export
db_copy_to.ClickhouseConnection <- function(con, table, values,
                                            overwrite = FALSE, types = NULL, temporary = TRUE,
                                            unique_indexes = NULL, indexes = NULL,
                                            analyze = FALSE, all_nullable = FALSE, ...) {

  if(analyze == TRUE){
    warning("clickhouse does not support a analyze statement.")
  }
  if(!is.null(unique_indexes)){
    warning("clickhouse does not support unique indexes.")
  }
  if(!is.null(indexes)){
    warning("clickhouse does not support indexes.")
  }

  if(is.null(types)){
    types <- dplyr::db_data_type(con, values)
  }

  names(types) <- names(values)

  if(all_nullable == TRUE){
    to_conv <- !1:length(types) %in% grep(types, pattern = "Nullable", value = FALSE)
    types[to_conv] <- paste0("Nullable(", types[to_conv], ")")
  }

  tryCatch({
    if (overwrite) {
      dplyr::db_drop_table(con, table, force = TRUE)
    }

    dplyr::db_write_table(con, table, types = types, values = values, temporary = temporary)
  }, error = function(err) {
    stop(err)
  })

  table
}

#' @export
#' @importFrom dbplyr sql_escape_logical
sql_escape_logical.ClickhouseConnection <- function(con, x) {
  if(is.na(x)) {
    return("NULL")
  } else {
    return(as.character(as.integer(x)))
  }
}

#' @importFrom dplyr sql_escape_ident
#' @importFrom dbplyr sql_vector
clickhouse_sql_join_vars <- function(con, vars) {
  if (any(duplicated(vars$alias))) {
    duplicatedVars <- vars$alias[duplicated(vars$alias)]
    stop("clickhouse only supports JOINs of tables with distinct column names, but the column names ",
         paste(duplicatedVars, collapse = ','), " occur in both tables. Please rename them.")
  }
  sql_vector(sql_escape_ident(con, vars$alias), parens = FALSE, collapse = ", ", con = con)
}

#' @importFrom dplyr sql_escape_ident
#' @importFrom dbplyr sql_vector
clickhouse_sql_join_tbls <- function(con, by) {
  on <- NULL
  if (length(by$x) + length(by$y) > 0) {
    on <- sql_vector(paste0(sql_escape_ident(con, by$x), " = ", sql_escape_ident(con, by$y)), collapse = " AND ", parens = TRUE, con = con)
  }
  on
}
