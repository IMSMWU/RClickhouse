serveraddr <- "127.0.0.1"
tblname    <- "test"

writeReadTest <- function(input, result = input, types = NULL) {
  skip_on_cran()
  conn <- dbConnect(RClickhouse::clickhouse(), host=serveraddr)
  dbWriteTable(conn, tblname, input, overwrite=T, field.types=types)
  r <- dbReadTable(conn, tblname)
  expect_equal(r, result)
  dbDisconnect(conn)
}
