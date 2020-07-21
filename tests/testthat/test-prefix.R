context("prefix")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)
library(dbplyr, warn.conflicts=F)

test <- data.frame(values = c(1,4,30,23.2,12,4,7,4,1,0,12,3))

source("utils.R")

test_that("custom aggregators translated correctly", {
  trans <- function(x) {
    translate_sql(!!enquo(x), window = FALSE, con = simulate_clickhouse())
  }

  expect_equal(trans(CaseSensitive(x)), sql("CaseSensitive(`x`)"))
  expect_equal(trans(sd(x)), sql("stddevSamp(`x`)"))
})
