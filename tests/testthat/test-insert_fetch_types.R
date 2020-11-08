context("insert_fetch_types")

library(DBI, warn.conflicts=F)

source("utils.R")

# DATE (BEGIN)
# test_that("write_read__Nullable(Date)", {
#
#   con <- getRealConnection()
#   dbExecute(con, 'create table NullableDateTable (`c` Nullable(Date)) ENGINE=MergeTree() ORDER BY tuple()')
#
#   date = data.frame(c = as.Date('2000-01-01'))
#   nullableDate = data.frame(c = as.Date(''))
#
#   testthat::expect_success(dbWriteTable(con, 'NullableDateTable', date, append = TRUE, overwrite = FALSE))
#   testthat::expect_success(dbWriteTable(con, 'NullableDateTable', nullableDate, append = TRUE, overwrite = FALSE))
#
#   # cleanup
#   dbRemoveTable(con, "NullableDateTable")
#   dbDisconnect(con)
# })
#
# test_that("write_read__Date", {
#   con <- getRealConnection()
#   dbExecute(con, 'create table DateTable (`c` Date) ENGINE=MergeTree() ORDER BY tuple()')
#
#   date = data.frame(c = as.Date('2000-01-01'))
#   nullableDate = data.frame(c = as.Date(''))
#
# testthat::expect_success(dbWriteTable(con, 'DateTable', date, append = TRUE, overwrite = FALSE))
# testthat::expect_failure(dbWriteTable(con, 'DateTable', nullableDate, append = TRUE, overwrite = FALSE))
#
#   # cleanup
#   dbRemoveTable(con, "DateTable")
#   dbDisconnect(con)
# })
# DATE (END)

