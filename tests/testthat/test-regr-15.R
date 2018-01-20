context("regr-15")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)
library(dbplyr, warn.conflicts=F)

source("utils.R")

#test_that("correct conversion of logical values in dplyr (#15)", {
#  skip_on_cran()
#  conn <- getRealConnection()
#  dbWriteTable(conn, tblname, data.frame(x=c(20,21,22)), overwrite=T)
#  expect_equal(as.data.frame(tbl(conn, tblname) %>% summarize(isGt = ifelse(x > 20, TRUE, FALSE))),
#               data.frame(isGt=c(0,1,1)))
#  dbDisconnect(conn)
#})
