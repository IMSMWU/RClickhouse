context("ClickhouseConnection")

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)

source("utils.R")

test_that("show__ClickhouseConnection", {
  conn <- getRealConnection()
  targetString <- '<ClickhouseConnection>  default@localhost:9000'
  expect_equal(paste(capture.output(RClickhouse::show(conn)), collapse = ''), targetString)

  # cleanup
  dbDisconnect(conn)
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

test_that("dbListTables__ClickhouseConnection", {
  conn <- getRealConnection()
  dbWriteTable(conn, "tableOne", 1:4, overwrite=T)
  dbWriteTable(conn, "tableTwo", 1:4, overwrite=T)

  targetString <- '[1] "tableOne" "tableTwo"'
  expect_equal(paste(capture.output(dbListTables(conn)), collapse = ''), targetString)
  # cleanup
  dbRemoveTable(conn,"tableOne")
  dbRemoveTable(conn,"tableTwo")
  dbDisconnect(conn)
})

test_that("dbExistsTable__ClickhouseConnection", {
  conn <- getRealConnection()
  # before Table is created should return False
  expect_false(dbExistsTable(conn, 'test_dbExistsTable'))
  dbExecute(conn, "CREATE TABLE test_dbExistsTable (`name` String) ENGINE = TinyLog")
  # after Table is created should return True
  expect_true(dbExistsTable(conn, 'test_dbExistsTable'))
  # cleanup...
  dbRemoveTable(conn, "test_dbExistsTable")
  dbDisconnect(conn)
})

test_that("dbReadTable__ClickhouseConnection", {
  conn <- getRealConnection()
  dbWriteTable(conn, "test_dbReadTable", 1:4, overwrite=T)
  # checks if read values same as input values
  testthat::expect_equivalent(dbReadTable(conn, "test_dbReadTable"), data.frame(x=1:4))
  # cleanup
  dbRemoveTable(conn, "test_dbReadTable")
  dbDisconnect(conn)
})

test_that("dbRemoveTable__ClickhouseConnection", {
  conn <- getRealConnection()
  dbExecute(conn, "CREATE TABLE test_dbRemoveTable (`name` String) ENGINE = TinyLog")
  assertthat::assert_that(dbExistsTable(conn, 'test_dbRemoveTable'), msg='precondition for test failed, "test_dbRemoveTable" was not created')

  dbRemoveTable(conn, "test_dbRemoveTable")
  expect_false(dbExistsTable(conn, 'test_dbRemoveTable'))
  # cleanup
  dbDisconnect(conn)
})

# test_that("dbListFields__ClickhouseConnection", {
# })
#
# test_that("dbSendQuery__ClickhouseConnection", {
# })
#
# test_that("dbWriteTable__ClickhouseConnection", {
# })

test_that("dbCreateTable", {
  conn <- getRealConnection()

  tablename <- "PersonalInfo"
  fields <- c(Name="String",ID="Int32",Age="Int8",Profession="String")
  dbCreateTable(conn, tablename, fields=fields, overwrite=TRUE)

  expect_equal(tablename,dbListTables(conn))
  concatenatedOutput = '[1] \"Name\"       \"ID\"         \"Age\"        \"Profession\"'
  expect_equal(paste(capture.output(RClickhouse::dbListFields(conn,"PersonalInfo")), collapse = ''),concatenatedOutput)
  # cleanup
  dbRemoveTable(conn,"PersonalInfo")
  dbDisconnect(conn)
})

test_that("dbAppendTable", {
  conn <- getRealConnection()

  tablename <- "PersonalInfo"
  fields <- c(Name="String",ID="Int32",Age="Int8",Profession="String")
  dbCreateTable(conn, tablename, fields=fields, overwrite=TRUE)

  NameVALUES=c('John Smith','Peter Norwind',"Alexander Mightsby")
  IDVALUES=c(1:3)
  AgeVALUES=c(25,22,26)
  ProfessionVALUES=c('Software Engineer','Software Engineer','Assistant Software Engineer')
  appendThis <- data.frame(Name=NameVALUES,ID=IDVALUES,Age=AgeVALUES,Profession=ProfessionVALUES)
  dbAppendTable(conn, tablename, value=appendThis, row.names=FALSE)

  afterRead <- dbReadTable(conn, "PersonalInfo")
  expect_true(all(appendThis==afterRead))
  # cleanup
  RClickhouse::dbRemoveTable(conn,"PersonalInfo")
  dbDisconnect(conn)
})

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

