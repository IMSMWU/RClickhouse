context("testdriven_development")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)

source("utils.R")

test_that("ISSUE #71", {
  conn <- getRealConnection()

  tablename <- "PersonalInfo"
  columns <- as.data.frame(data.frame('aName','Age','Profession'))
  types <- c("String","Float64","String")

  dbCreateTable(conn, tablename, fields=columns, overwrite=TRUE, field.types=types)

  # after <- dbReadTable(conn, "PersonalInfo")



  # expect_equal(before, after)
  RClickhouse::dbRemoveTable(conn,"PersonalInfo")
})

test_that("ISSUE #71 dbAppendTable", {
  conn <- getRealConnection()

  tablename <- "PersonalInfo"
  columns <- as.data.frame(data.frame('aName','Age','Profession'))
  types <- c("String","Float64","String")

  dbCreateTable(conn, tablename, fields=columns, overwrite=TRUE, field.types=types)

  values <- columns
  values$aName <- "John Smith"
  values$Age <- 25
  values$aName <- "Software Developer"
  dbAppendTable(conn, tablename, value=values, row.names=NA)

  # after <- dbReadTable(conn, "PersonalInfo")



  # expect_equal(before, after)
  RClickhouse::dbRemoveTable(conn,"PersonalInfo")
})








