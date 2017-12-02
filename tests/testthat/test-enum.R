context("enum")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)  # for data_frame

source("utils.R")

test_that("reading & writing enum columns", {
  writeReadTest(as.data.frame(data_frame(x=as.factor(c("foo","bar","baz","foo","baz")))))
})

test_that("nullable enum column", {
  writeReadTest(as.data.frame(data_frame(x=as.factor(c("foo",NA,"baz","foo","baz",NA)))))
})
