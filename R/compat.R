# Backward-compatibility layer ------------------------------------------------
#
# RClickhouse 2.0 is a clean break (Arrow transports instead of the native C++
# client), but we keep the previous dbConnect() arguments working with a
# deprecation warning so existing user code does not fail hard.

# Inspect dbConnect() dots for removed arguments, warn, and strip them. Returns
# the cleaned dots, with `$dbname` set if the legacy `db` argument was used.
#' @noRd
handle_deprecated_connect_args <- function(dots) {
  result <- dots

  if (!is.null(dots$db)) {
    lifecycle::deprecate_warn("2.0.0", "dbConnect(db = )", "dbConnect(dbname = )")
    result$dbname <- dots$db
    result$db <- NULL
  }

  if (!is.null(dots$compression)) {
    lifecycle::deprecate_warn(
      "2.0.0", "dbConnect(compression = )",
      details = "Arrow transports negotiate compression automatically; the argument is ignored."
    )
    result$compression <- NULL
  }

  if (!is.null(dots$Int64)) {
    lifecycle::deprecate_warn(
      "2.0.0", "dbConnect(Int64 = )",
      details = "64-bit integers are now mapped by Arrow (to bit64::integer64)."
    )
    result$Int64 <- NULL
  }

  if (!is.null(dots$toUTF8)) {
    lifecycle::deprecate_warn(
      "2.0.0", "dbConnect(toUTF8 = )",
      details = "Strings are always UTF-8 via Arrow."
    )
    result$toUTF8 <- NULL
  }

  result
}
