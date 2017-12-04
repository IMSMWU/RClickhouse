context("regr-25")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)

source("utils.R")

test_that("correct conversion of Date values", {
  data = as.data.frame(data_frame(x = c(as.Date('1994-03-15'),
                                        as.Date('2000-01-01'),
                                        as.Date('2012-04-22'),
                                        as.Date('1977-12-23'))))
  writeReadTest(data)
})
