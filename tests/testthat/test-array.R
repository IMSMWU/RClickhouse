context("array")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)  # for data_frame

source("utils.R")

test_that("reading & writing array columns", {
  writeReadTest(as.data.frame(data_frame(x=list(c(1,3,5),c(1,2)))))
})

test_that("nullable array columns", {
  writeReadTest(as.data.frame(data_frame(x=list(c(1,NA,3,5),c(1,2,NA)))))
})

test_that("array columns with empty entries", {
  writeReadTest(as.data.frame(data_frame(x=list(c(1,2,3),as.numeric(c()),c(4,5)))))
})
