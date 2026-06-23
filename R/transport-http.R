# HTTP + ArrowStream transport ------------------------------------------------
#
# Pure-R backend: talks to the ClickHouse HTTP interface with `httr2` and uses
# the `ArrowStream` format for both reads and writes, deserialised by
# `nanoarrow`. Works against any ClickHouse server (including ClickHouse Cloud)
# and needs no compiled driver, which makes it the CRAN-out-of-the-box path.

#' @noRd
ch_http_connect <- function(host, port, dbname, user, password,
                            tls = FALSE, timeout = 60, ...) {
  scheme <- if (isTRUE(tls)) "https" else "http"
  base_url <- sprintf("%s://%s:%s/", scheme, host, port)

  handle <- structure(
    list(
      base_url = base_url,
      dbname   = dbname,
      user     = user,
      password = password,
      timeout  = timeout
    ),
    class = c("ch_http_transport", "ch_transport")
  )
  handle
}

# Build a request carrying auth, target database and ClickHouse error handling.
#' @noRd
http_request <- function(transport, query_params = list()) {
  params <- c(list(database = transport$dbname), query_params)
  req <- httr2::request(transport$base_url)
  req <- httr2::req_headers(req,
    `X-ClickHouse-User` = transport$user,
    `X-ClickHouse-Key`  = transport$password
  )
  req <- httr2::req_url_query(req, !!!params)
  req <- httr2::req_timeout(req, transport$timeout)
  # ClickHouse returns the error text in the response body; surface it.
  httr2::req_error(req, body = function(resp) httr2::resp_body_string(resp))
}

#' @noRd
ch_query_stream.ch_http_transport <- function(transport, statement, ...) {
  # nanoarrow's IPC reader cannot decode compressed Arrow buffers, so ask
  # ClickHouse to emit an uncompressed ArrowStream.
  req <- http_request(transport,
    query_params = list(output_format_arrow_compression_method = "none"))
  req <- httr2::req_body_raw(
    req, charToRaw(ch_format_clause(statement, "ArrowStream")),
    type = "text/plain; charset=utf-8"
  )
  resp <- httr2::req_perform(req)
  nanoarrow::read_nanoarrow(httr2::resp_body_raw(resp))
}

#' @noRd
ch_execute.ch_http_transport <- function(transport, statement, ...) {
  req <- http_request(transport)
  req <- httr2::req_body_raw(req, charToRaw(statement),
                            type = "text/plain; charset=utf-8")
  resp <- httr2::req_perform(req)
  http_rows_affected(resp)
}

#' @noRd
ch_insert.ch_http_transport <- function(transport, name, value, ...) {
  query <- paste0("INSERT INTO ", quote_ident_backtick(name), " FORMAT ArrowStream")
  body <- arrow_ipc_bytes(value)

  req <- http_request(transport, query_params = list(query = query))
  req <- httr2::req_body_raw(req, body, type = "application/octet-stream")
  resp <- httr2::req_perform(req)
  invisible(http_rows_affected(resp, default = nrow(value)))
}

#' @noRd
ch_close.ch_http_transport <- function(transport, ...) {
  invisible(TRUE) # stateless
}

#' @noRd
ch_valid.ch_http_transport <- function(transport, ...) {
  tryCatch({
    as.data.frame(ch_query_stream(transport, "SELECT 1"))
    TRUE
  }, error = function(e) FALSE)
}

# Helpers ---------------------------------------------------------------------

# Serialise a data.frame to Arrow IPC stream bytes via nanoarrow.
#' @noRd
arrow_ipc_bytes <- function(value) {
  con <- rawConnection(raw(0), "wb")
  on.exit(close(con), add = TRUE)
  nanoarrow::write_nanoarrow(value, con)
  rawConnectionValue(con)
}

# ClickHouse reports row counts in the X-ClickHouse-Summary header (JSON).
#' @noRd
http_rows_affected <- function(resp, default = NA_real_) {
  summary <- httr2::resp_header(resp, "X-ClickHouse-Summary")
  if (is.null(summary)) return(default)
  parsed <- tryCatch(jsonlite_fromJSON(summary), error = function(e) NULL)
  written <- suppressWarnings(as.numeric(parsed[["written_rows"]]))
  if (length(written) == 1 && !is.na(written)) written else default
}

# Minimal JSON object reader for the flat summary header, avoiding a hard
# jsonlite dependency.
#' @noRd
jsonlite_fromJSON <- function(txt) {
  pairs <- regmatches(txt, gregexpr('"[^"]+"\\s*:\\s*"[^"]*"', txt))[[1]]
  out <- list()
  for (p in pairs) {
    m <- regmatches(p, regexec('"([^"]+)"\\s*:\\s*"([^"]*)"', p))[[1]]
    out[[m[2]]] <- m[3]
  }
  out
}
