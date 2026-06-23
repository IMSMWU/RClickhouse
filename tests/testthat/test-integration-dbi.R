# DBI round-trip tests against a live ClickHouse server.

test_that("scalar query returns expected columns", {
  con <- local_connection()
  res <- dbGetQuery(con, "SELECT 1 AS a, 2.5 AS b, 'x' AS c")
  expect_equal(res$a, 1)
  expect_equal(res$b, 2.5)
  expect_equal(res$c, "x")
})

test_that("write / list / read / remove table round-trips", {
  con <- local_connection()
  tbl <- tmp_table()
  on.exit(try(dbRemoveTable(con, tbl), silent = TRUE), add = TRUE)

  dbWriteTable(con, tbl, mtcars, overwrite = TRUE, row.names = FALSE)
  expect_true(dbExistsTable(con, tbl))
  expect_true(tbl %in% dbListTables(con))
  expect_setequal(dbListFields(con, tbl), names(mtcars))

  back <- dbReadTable(con, tbl)
  expect_equal(nrow(back), nrow(mtcars))
  back <- back[order(back$mpg), names(mtcars)]
  orig <- mtcars[order(mtcars$mpg), ]
  rownames(back) <- NULL; rownames(orig) <- NULL
  expect_equal(back, orig, ignore_attr = TRUE)

  dbRemoveTable(con, tbl)
  expect_false(dbExistsTable(con, tbl))
})

test_that("dbCreateTable + dbAppendTable work", {
  con <- local_connection()
  tbl <- tmp_table()
  on.exit(try(dbRemoveTable(con, tbl), silent = TRUE), add = TRUE)

  fields <- c(Name = "String", ID = "Int32", Age = "Int8")
  dbCreateTable(con, tbl, fields)
  expect_true(dbExistsTable(con, tbl))

  value <- data.frame(
    Name = c("Ada", "Alan"), ID = 1:2L, Age = c(36L, 41L),
    stringsAsFactors = FALSE
  )
  n <- dbAppendTable(con, tbl, value)
  expect_equal(n, 2)
  back <- dbReadTable(con, tbl)
  expect_setequal(back$Name, value$Name)
})

test_that("overwrite and append guards behave", {
  con <- local_connection()
  tbl <- tmp_table()
  on.exit(try(dbRemoveTable(con, tbl), silent = TRUE), add = TRUE)

  dbWriteTable(con, tbl, data.frame(x = 1:3), row.names = FALSE)
  expect_error(dbWriteTable(con, tbl, data.frame(x = 1:3), row.names = FALSE),
               "already exists")
  dbWriteTable(con, tbl, data.frame(x = 4:6), append = TRUE, row.names = FALSE)
  expect_equal(dbGetQuery(con, paste("SELECT count() AS n FROM", tbl))$n, 6)
})

test_that("dbSendQuery / dbFetch / dbColumnInfo work", {
  con <- local_connection()
  res <- dbSendQuery(con, "SELECT number AS n FROM numbers(10)")
  on.exit(dbClearResult(res), add = TRUE)

  first <- dbFetch(res, 3)
  expect_equal(nrow(first), 3)
  expect_false(dbHasCompleted(res))
  rest <- dbFetch(res)
  expect_equal(nrow(rest), 7)
  expect_true(dbHasCompleted(res))

  info <- dbColumnInfo(res)
  expect_equal(info$name, "n")
})

test_that("transactions are rejected", {
  con <- local_connection()
  expect_error(dbBegin(con), "not supported")
  expect_error(dbCommit(con), "not supported")
  expect_error(dbRollback(con), "not supported")
})
