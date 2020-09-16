context("ClickhouseConnection")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)

source("utils.R")

test_that("show__ClickhouseConnection", {
  conn <- getRealConnection()
  targetString <- '<ClickhouseConnection>  default@localhost:9000'
  expect_equal(paste(capture.output(RClickhouse::show(conn)), collapse = ''), targetString)
})

# # not sure on how to adequately test this method
# test_that("dbGetInfo__ClickhouseConnection", {
#   # conn <- getRealConnection()
#   # target <- dbGetQuery(conn, "SELECT version() as version, uptime() as uptime,
#   #                       currentDatabase() as database")
#   # targetString <- '<ClickhouseConnection>  default@localhost:9000'
#   # expect_equal(RClickhouse::dbGetInfo(conn), list(name="ClickhouseConnection",db.version="20.1.6.30",uptime=target$uptime,dbname=target$database,username="default",host="localhost",port=9000))
# })

# # not sure on how to adequately test this method
# test_that("dbIsValid__ClickhouseConnection", {
# })

# test_that("dbListTables__ClickhouseConnection", {
#   conn <- getRealConnection()
#   dbWriteTable(conn, "tableOne", 1:4, overwrite=T)
#   dbWriteTable(conn, "tableTwo", 1:4, overwrite=T)
#
#   targetString <- '[1] "tableOne" "tableTwo"'
#   expect_equal(paste(capture.output(RClickhouse::dbListTables(conn)), collapse = ''), targetString)
#   RClickhouse::dbRemoveTable(conn,"tableOne")
#   RClickhouse::dbRemoveTable(conn,"tableTwo")
# })

# test_that("dbExistsTable__ClickhouseConnection", {
# })
#
# test_that("dbReadTable__ClickhouseConnection", {
# })
#
# test_that("dbRemoveTable__ClickhouseConnection", {
# })
#
# test_that("dbListFields__ClickhouseConnection", {
# })
#
# test_that("dbSendQuery__ClickhouseConnection", {
# })
#
# test_that("dbWriteTable__ClickhouseConnection", {
# })
#
# test_that("dbDataType__ClickhouseConnection", {
# })
#
# test_that("dbQuoteIdentifier__ClickhouseConnection", {
# })
#
# test_that("dbQuoteString__ClickhouseConnection_1", {
# })
#
# test_that("dbQuoteString__ClickhouseConnection_2", {
# })
#
# test_that("dbBegin__ClickhouseConnection", {
# })
#
# test_that("dbCommit__ClickhouseConnection", {
# })
#
# test_that("dbRollback__ClickhouseConnection", {
# })
#
# test_that("dbDisconnect__ClickhouseConnection", {
# })

