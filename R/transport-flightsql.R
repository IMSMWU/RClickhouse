# Arrow Flight SQL transport (ADBC) -------------------------------------------
#
# Talks to ClickHouse's Arrow Flight SQL endpoint through the ADBC driver
# manager. Reads come back as nanoarrow array streams; writes use ADBC bulk
# ingest. No type marshalling is done here -- Arrow is the shared type system.

#' @noRd
ch_flightsql_connect <- function(host, port, dbname, user, password,
                                 tls = FALSE, ...) {
  rlang::check_installed(
    c("adbcdrivermanager", "adbcflightsql"),
    reason = paste0(
      "to use the Arrow Flight SQL transport.\n",
      "The Flight SQL driver is distributed via R-multiverse:\n",
      '  install.packages("adbcflightsql", repos = "https://community.r-multiverse.org")\n',
      "Alternatively connect with transport = \"http\"."
    )
  )

  scheme <- if (isTRUE(tls)) "grpc+tls" else "grpc"
  uri <- sprintf("%s://%s:%s", scheme, host, port)

  # ClickHouse's Flight SQL basic-auth handshake only succeeds for users that
  # actually have a password; the passwordless default user must connect with
  # no auth options at all. Only send credentials when a password is set.
  db_args <- list(adbcflightsql::adbcflightsql(), uri = uri)
  if (nzchar(password)) {
    db_args$username <- user
    db_args$password <- password
  }

  db <- do.call(adbcdrivermanager::adbc_database_init, db_args)
  con <- adbcdrivermanager::adbc_connection_init(db)

  handle <- structure(
    list(database = db, connection = con, dbname = dbname),
    class = c("ch_flightsql_transport", "ch_transport")
  )

  # ClickHouse Flight SQL exposes the active database via a session setting;
  # switch to the requested one if it isn't the server default.
  if (!is.null(dbname) && nzchar(dbname) && dbname != "default") {
    ch_execute(handle, paste0("USE ", quote_ident_backtick(dbname)))
  }
  handle
}

#' @noRd
ch_query_stream.ch_flightsql_transport <- function(transport, statement, ...) {
  # dbplyr passes classed SQL objects; ADBC needs a plain string.
  adbcdrivermanager::read_adbc(transport$connection, as.character(statement))
}

#' @noRd
ch_execute.ch_flightsql_transport <- function(transport, statement, ...) {
  out <- adbcdrivermanager::execute_adbc(transport$connection, as.character(statement))
  affected <- attr(out, "rows_affected", exact = TRUE)
  if (is.null(affected)) NA_real_ else as.numeric(affected)
}

#' @noRd
ch_insert.ch_flightsql_transport <- function(transport, name, value, ...) {
  # The target table is created up-front by dbWriteTable/dbCreateTable (so the
  # ClickHouse ENGINE can be set), hence we always append here.
  adbcdrivermanager::write_adbc(
    value, transport$connection, target_table = name, mode = "append"
  )
  invisible(nrow(value))
}

#' @noRd
ch_close.ch_flightsql_transport <- function(transport, ...) {
  try(adbcdrivermanager::adbc_connection_release(transport$connection), silent = TRUE)
  try(adbcdrivermanager::adbc_database_release(transport$database), silent = TRUE)
  invisible(TRUE)
}

#' @noRd
ch_valid.ch_flightsql_transport <- function(transport, ...) {
  tryCatch({
    as.data.frame(ch_query_stream(transport, "SELECT 1"))
    TRUE
  }, error = function(e) FALSE)
}
