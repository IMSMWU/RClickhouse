serveraddr <- "127.0.0.1"
tblname    <- "test"

getRealConnection <- function(){
  # TODO: recycle connection by using singleton?
  conn <- dbConnect(RClickhouse::clickhouse(), host=serveraddr)
  return (conn)
}

writeReadTest <- function(input, result = input, types = NULL) {
  skip_on_cran()
  conn <- getRealConnection()

  dbWriteTable(conn, tblname, input, overwrite=T, field.types=types)
  r <- dbReadTable(conn, tblname)
  expect_equal(r, result)
  dbDisconnect(conn)
}
