context("int64")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)  # for data_frame

source("utils.R")

test_that("reading & writing int64", {
  writeReadTest(as.data.frame(data_frame(x=bit64::as.integer64(c("9007199254740993")))))
})

