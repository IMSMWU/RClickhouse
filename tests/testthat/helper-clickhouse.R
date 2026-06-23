# Shared test helpers.
#
# Integration tests connect to a ClickHouse server configured via environment
# variables (see docker-compose.yml for a local instance). They are skipped on
# CRAN and whenever no server is reachable.

test_transport <- function() {
  Sys.getenv("RCLICKHOUSE_TEST_TRANSPORT", "http")
}

test_connection <- function() {
  skip_on_cran()
  transport <- test_transport()
  if (transport == "flightsql") {
    skip_if_not_installed("adbcflightsql")
    skip_if_not_installed("adbcdrivermanager")
  }
  host <- Sys.getenv("RCLICKHOUSE_TEST_HOST", "localhost")
  port <- Sys.getenv("RCLICKHOUSE_TEST_PORT", "")
  user <- Sys.getenv("RCLICKHOUSE_TEST_USER", "default")
  pass <- Sys.getenv("RCLICKHOUSE_TEST_PASSWORD", "")

  args <- list(RClickhouse::clickhouse(), host = host, user = user,
               password = pass, transport = transport)
  if (nzchar(port)) args$port <- as.integer(port)

  con <- tryCatch(do.call(DBI::dbConnect, args), error = function(e) NULL)
  if (is.null(con) || !DBI::dbIsValid(con)) {
    skip(paste0("No reachable ClickHouse (", transport, ") for integration tests"))
  }
  con
}

# A connection scoped to the calling test: disconnected automatically.
local_connection <- function(env = parent.frame()) {
  con <- test_connection()
  withr::defer(DBI::dbDisconnect(con), envir = env)
  con
}

# Unique table name to avoid clashes between parallel runs.
tmp_table <- function(prefix = "rch_test") {
  paste0(prefix, "_", as.integer(stats::runif(1, 1, 1e8)))
}
