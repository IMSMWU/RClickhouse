context("uuid")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)  # for data_frame

source("utils.R")

test_that("reading & writing UUID columns", {
  writeReadTest(as.data.frame(data_frame(a=as.character(c(
      "049b9423-38e3-4722-a9f6-b98c0dc87190",
      "67de9c22-bb18-4d4d-b47f-8cf795ab4709",
      "b59c08f5-4a20-4f0e-8e32-a48a61732635",
      "2de7c296-8826-4579-b05a-5c4c136360dd",
      "3b8520e5-57e6-426b-9649-cf4de4709079")))), types=c("UUID"))
})

test_that("reading & writing Nullable UUID columns", {
  writeReadTest(as.data.frame(data_frame(b=as.character(c(
      "049b9423-38e3-4722-a9f6-b98c0dc87190",
      NA,
      "b59c08f5-4a20-4f0e-8e32-a48a61732635",
      "2de7c296-8826-4579-b05a-5c4c136360dd",
      NA)))), types=c("Nullable(UUID)"))
})

test_that("error on incorrect UUID format", {
  skip_on_cran()
  conn <- getRealConnection()
  input <- c("049b9423-38e3-4722-a9f6-b98c0dc87190", "0123def")
  expect_error(dbWriteTable(conn, tblname, input, overwrite=T, field.types=c("UUID")))
  RClickhouse::dbRemoveTable(conn,tblname)
  dbDisconnect(conn)
})
