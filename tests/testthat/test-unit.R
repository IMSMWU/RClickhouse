# Unit tests that need no running server.

test_that("driver construction and metadata", {
  drv <- RClickhouse::clickhouse()
  expect_s4_class(drv, "ClickhouseDriver")
  expect_true(dbIsValid(drv))
  expect_type(dbGetInfo(drv), "list")
})

test_that("dbDataType maps R types to ClickHouse types", {
  drv <- RClickhouse::clickhouse()
  expect_equal(dbDataType(drv, 1L), "Int32")
  expect_equal(dbDataType(drv, 1.5), "Float64")
  expect_equal(dbDataType(drv, TRUE), "UInt8")
  expect_equal(dbDataType(drv, "a"), "String")
  expect_equal(dbDataType(drv, bit64::as.integer64(1)), "Int64")
  expect_equal(dbDataType(drv, as.Date("2020-01-01")), "Date")
  expect_equal(dbDataType(drv, factor(c("a", "b"))), "Enum16('a'=0,'b'=1)")
  expect_equal(dbDataType(drv, c(1L, NA)), "Nullable(Int32)")
})

test_that("identifier and string quoting follow ClickHouse rules", {
  con <- new("ClickhouseConnection")
  expect_equal(as.character(dbQuoteIdentifier(con, "tbl")), "`tbl`")
  expect_equal(as.character(dbQuoteIdentifier(con, "a`b")), "`a\\`b`")
  expect_equal(as.character(dbQuoteString(con, "x")), "'x'")
  expect_equal(as.character(dbQuoteString(con, "a'b")), "'a\\'b'")
  expect_error(dbQuoteIdentifier(con, NA_character_))
})

test_that("default ports depend on transport", {
  expect_equal(RClickhouse:::default_port("flightsql"), 9090L)
  expect_equal(RClickhouse:::default_port("http"), 8123L)
  expect_equal(RClickhouse:::default_port("http", tls = TRUE), 8443L)
})

test_that("config resolution honours precedence", {
  cfg_file <- withr::local_tempfile(fileext = ".yaml")
  writeLines(c("host: confighost", "transport: http", "dbname: fromfile"), cfg_file)

  cfg <- RClickhouse:::ch_resolve_config(cfg_file, list(
    host = "localhost", port = NULL, dbname = "default", user = "default",
    password = "", transport = "flightsql", tls = FALSE))
  # explicit default-valued args do not override the file; file wins
  expect_equal(cfg$host, "confighost")
  expect_equal(cfg$transport, "http")
  expect_equal(cfg$dbname, "fromfile")

  # an explicitly non-default arg beats the file
  cfg2 <- RClickhouse:::ch_resolve_config(cfg_file, list(
    host = "explicit", port = NULL, dbname = "default", user = "default",
    password = "", transport = "flightsql", tls = FALSE))
  expect_equal(cfg2$host, "explicit")
})

test_that("deprecated connect arguments warn and are stripped", {
  expect_snapshot_warning(
    out <- RClickhouse:::handle_deprecated_connect_args(list(compression = "lz4"))
  )
  out <- suppressWarnings(
    RClickhouse:::handle_deprecated_connect_args(list(db = "mydb", Int64 = "integer"))
  )
  expect_equal(out$dbname, "mydb")
  expect_false("db" %in% names(out))     # $ would partial-match dbname
  expect_false("Int64" %in% names(out))
})

test_that("http transport object is well formed", {
  h <- RClickhouse:::ch_http_connect("localhost", 8123, "default", "default", "")
  expect_s3_class(h, "ch_http_transport")
  expect_equal(h$base_url, "http://localhost:8123/")
})

test_that("sql_translation renders ClickHouse-specific SQL", {
  con <- dbplyr::simulate_dbi("ClickhouseConnection")
  # ^ maps to the case-sensitive pow(); var()/sd() to the *Samp() aggregates.
  expect_match(as.character(dbplyr::translate_sql(x^2, con = con)),
               "^pow\\(", perl = TRUE)
  expect_match(as.character(dbplyr::translate_sql(var(x), con = con, window = FALSE)),
               "^varSamp\\(", perl = TRUE)
  expect_match(as.character(dbplyr::translate_sql(sd(x), con = con, window = FALSE)),
               "^stddevSamp\\(", perl = TRUE)
})
