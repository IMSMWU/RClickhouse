serveraddr <- "127.0.0.1"
tblname    <- "test"

writeReadTest <- function(input, result = input) {
  skip_on_cran()
  conn <- dbConnect(RClickhouse::clickhouse(), host=serveraddr)
  dbWriteTable(conn, tblname, input, overwrite=T)
  r <- dbReadTable(conn, tblname)
  expect_equal(r, result)
  dbDisconnect(conn)
}
