# Transport seam --------------------------------------------------------------
#
# A "transport" is the low-level mechanism that ships SQL to ClickHouse and
# Arrow data back. Each transport is an S3 object (subclass of `ch_transport`)
# carrying whatever handle/state it needs. All DBI methods in the package are
# written against the generics below and never touch a transport's internals,
# so adding or swapping a backend is a self-contained change.
#
# Implementations:
#   * ch_flightsql_transport  (transport-flightsql.R) -- ADBC / Arrow Flight SQL
#   * ch_http_transport       (transport-http.R)      -- HTTP + ArrowStream

#' Open a transport for the requested backend.
#'
#' Returns an object inheriting from `ch_transport`. Dispatch is by `transport`
#' string rather than S3 class because no object exists yet.
#' @noRd
ch_transport_connect <- function(transport = c("flightsql", "http"),
                                 host, port, dbname, user, password,
                                 tls = FALSE, ...) {
  transport <- match.arg(transport)
  switch(transport,
    flightsql = ch_flightsql_connect(host = host, port = port, dbname = dbname,
                                     user = user, password = password,
                                     tls = tls, ...),
    http      = ch_http_connect(host = host, port = port, dbname = dbname,
                               user = user, password = password,
                               tls = tls, ...)
  )
}

# Generics --------------------------------------------------------------------

#' Run a query and return its result as a nanoarrow array stream.
#' @noRd
ch_query_stream <- function(transport, statement, ...) {
  UseMethod("ch_query_stream")
}

#' Run a query and return its result materialised as a data.frame.
#' @noRd
ch_query <- function(transport, statement, ...) {
  UseMethod("ch_query")
}

#' Execute a statement that returns no result set; return rows affected (or NA).
#' @noRd
ch_execute <- function(transport, statement, ...) {
  UseMethod("ch_execute")
}

#' Bulk-insert a data.frame into an existing table.
#' @noRd
ch_insert <- function(transport, name, value, ...) {
  UseMethod("ch_insert")
}

#' Close the transport and release its resources.
#' @noRd
ch_close <- function(transport, ...) {
  UseMethod("ch_close")
}

#' Cheap liveness check.
#' @noRd
ch_valid <- function(transport, ...) {
  UseMethod("ch_valid")
}

# Default: materialise the stream from `ch_query_stream()`. Transports that have
# a cheaper direct path may override `ch_query`.
#' @noRd
ch_query.default <- function(transport, statement, ...) {
  materialize_stream(ch_query_stream(transport, statement, ...))
}

# Convert a nanoarrow array stream to a data.frame. nanoarrow maps Arrow int64
# to R double by default, which loses precision for large values; map ClickHouse
# Int64/UInt64 columns to bit64::integer64 instead. All other types use
# nanoarrow's inferred default conversion.
#' @noRd
materialize_stream <- function(stream) {
  schema <- nanoarrow::infer_nanoarrow_schema(stream)
  ptype  <- nanoarrow::infer_nanoarrow_ptype(schema)
  formats <- vapply(schema$children, function(ch) ch$format, character(1))
  is_int64 <- formats %in% c("l", "L")  # arrow int64 / uint64
  if (any(is_int64)) {
    for (i in which(is_int64)) ptype[[i]] <- bit64::integer64()
  }
  nanoarrow::convert_array_stream(stream, to = ptype)
}

# Shared helpers --------------------------------------------------------------

#' @noRd
ch_format_clause <- function(statement, format) {
  paste0(statement, "\nFORMAT ", format)
}

# Low-level ClickHouse quoting, shared by transports and the DBI quoting
# methods. ClickHouse quotes identifiers with backticks and strings with single
# quotes, escaping backslashes and the quote character itself.
#' @noRd
quote_ident_backtick <- function(x) {
  x <- gsub("\\", "\\\\", x, fixed = TRUE)
  x <- gsub("`", "\\`", x, fixed = TRUE)
  paste0("`", x, "`")
}

#' @noRd
quote_string_sql <- function(x) {
  x <- gsub("\\", "\\\\", x, fixed = TRUE)
  x <- gsub("'", "\\'", x, fixed = TRUE)
  ifelse(is.na(x), "NULL", paste0("'", x, "'"))
}
