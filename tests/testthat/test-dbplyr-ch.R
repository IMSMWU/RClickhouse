context("dbplyr-ch")

# ToDo: make the tests with SQL string comparisons only, since actual DB operations are not necessary
# just use show_query() and to comparison, organize SQL-Strings in directory...
# ToDo: fix problem where you have to import directly dplyr::    why suddenly not working anymore?

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)

source("utils.R")

conn <- getRealConnection()

tableA <- data.frame(
  "ID"=c(1,2),
  "A"=c('a1', 'a2')
)
tableB <- data.frame(
  "ID"=c(2,3),
  "B"=c('b2', 'b3')
)
tableC <- data.frame(
  "KEY"=c(1,5),
  "C"=c('c1', 'c5')
)

DBI::dbWriteTable(conn, "tableA", tableA, overwrite=TRUE)
DBI::dbWriteTable(conn, "tableB", tableB, overwrite=TRUE)
DBI::dbWriteTable(conn, "tableC", tableC, overwrite=TRUE)
pointerTableA <- tbl(conn, "tableA")
pointerTableB <- tbl(conn, "tableB")
pointerTableC <- tbl(conn, "tableC")


# Left_JOIN_Variations
test_that("left_join() between two tables WITH a common column", {
  expect_equal(data.frame(left_join(pointerTableA, pointerTableB)), data.frame(ID = c(1, 2), A = c('a1','a2'), B = c('','b2')))
})
test_that("left_join() with ON-clause between two tables WITH a common column", {
  expect_equal(data.frame(left_join(pointerTableA, pointerTableB, by = c('ID'))), data.frame(ID = c(1, 2), A = c('a1','a2'), B = c('','b2')))
})
test_that("left_join() with ON-clause between two tables WITHOUT a common column", {
  expect_equal(data.frame(left_join(pointerTableA, pointerTableC, by = c('ID'='KEY'))), data.frame(ID = c(1, 2), A = c('a1','a2'), C = c('c1','')))
})


# Recreate Problem from Issue #61
test_that("example form #61", {
  expect_equal(data.frame(pointerTableA  %>% dplyr::select(ID, A) %>% left_join(pointerTableB %>% dplyr::select(ID,B), by = c("ID"))), data.frame(ID = c(1, 2), A = c('a1','a2'), B = c('','b2')))
})
test_that("example form #61 with two tables in ON-clause", {
  expect_equal(data.frame(pointerTableA  %>% dplyr::select(ID, A) %>% left_join(pointerTableB %>% dplyr::select(ID,B), by = c("ID"="ID"))), data.frame(ID = c(1, 2), A = c('a1','a2'), B = c('','b2')))
})
  test_that("example form #61 without ON-clause", {
  expect_equal(data.frame(pointerTableA  %>% dplyr::select(ID, A) %>% left_join(pointerTableB %>% dplyr::select(ID,B))), data.frame(ID = c(1, 2), A = c('a1','a2'), B = c('','b2')))
})


# Complex SQL-Generations
test_that("3 left_joins", {
  expect_equal(data.frame(pointerTableA  %>% dplyr::select(ID, A) %>% left_join(pointerTableB %>% dplyr::select(B,ID)) %>% left_join(left_join(pointerTableA, pointerTableC, by = c('ID'='KEY')))), data.frame(ID = c(1, 2), A = c('a1','a2'), B = c('','b2'), C = c('c1','')))
})


# Various_JOINS
test_that("inner_join() between two tables WITH a common column", {
  expect_equal(data.frame(inner_join(pointerTableA, pointerTableB)), data.frame(ID = c(2), A = c('a2'), B = c('b2')))
})
test_that("right_join() 1/2 between two tables WITH a common column", {
  expect_equal(data.frame(right_join(pointerTableA, pointerTableB)), data.frame(ID = c(2, 3), A = c('a2', ''), B = c('b2', 'b3')))
})
test_that("right_join() 2/2 between two tables WITH a common column", {
  expect_equal(data.frame(right_join(pointerTableB, pointerTableA)), data.frame(ID = c(2, 1), B = c('b2', ''),  A = c('a2', 'a1')))
})
test_that("full_join() between two tables WITH a common column", {
  expect_equal(data.frame(full_join(pointerTableA, pointerTableB)), data.frame(ID = c(1, 2, 0), A = c('a1', 'a2', ''), B = c('','b2', 'b3')))
})


RClickhouse::dbRemoveTable(conn,"tableA")
RClickhouse::dbRemoveTable(conn,"tableB")
RClickhouse::dbRemoveTable(conn,"tableC")






