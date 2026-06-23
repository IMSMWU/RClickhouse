# Type round-trip tests against a live ClickHouse server. Arrow handles the
# conversion on both ends; these guard the common R types.

roundtrip <- function(con, value, field.types = NULL) {
  tbl <- tmp_table()
  on.exit(try(dbRemoveTable(con, tbl), silent = TRUE), add = TRUE)
  dbWriteTable(con, tbl, value, overwrite = TRUE, row.names = FALSE,
               field.types = field.types)
  dbReadTable(con, tbl)
}

test_that("integer and double round-trip", {
  con <- local_connection()
  back <- roundtrip(con, data.frame(i = 1:5L, d = c(1.5, 2.5, 3.5, 4.5, 5.5)))
  expect_equal(sort(back$i), 1:5L)
  expect_equal(sort(back$d), c(1.5, 2.5, 3.5, 4.5, 5.5))
})

test_that("64-bit integers round-trip via bit64", {
  con <- local_connection()
  x <- bit64::as.integer64(c(1, 2, 2^40))
  back <- roundtrip(con, data.frame(x = x), field.types = c(x = "Int64"))
  expect_true(bit64::is.integer64(back$x))
  expect_equal(sort(as.numeric(back$x)), sort(as.numeric(x)))
})

test_that("Date and POSIXct round-trip", {
  con <- local_connection()
  d <- as.Date(c("2020-01-01", "2021-06-15"))
  back <- roundtrip(con, data.frame(d = d), field.types = c(d = "Date"))
  expect_equal(sort(back$d), d)
})

test_that("strings round-trip as UTF-8", {
  con <- local_connection()
  s <- c("ascii", "Grüße", "日本語")
  back <- roundtrip(con, data.frame(s = s, stringsAsFactors = FALSE))
  expect_setequal(back$s, s)
})

test_that("nullable columns preserve NA", {
  con <- local_connection()
  back <- roundtrip(con, data.frame(x = c(1L, NA, 3L)),
                    field.types = c(x = "Nullable(Int32)"))
  expect_equal(sort(back$x, na.last = TRUE), c(1L, 3L, NA))
})
