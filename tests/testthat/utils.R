
# basic set-if-unset operator
"%||=%" <- function(x, y){
  Var <- deparse(substitute(x))
  if (!exists(Var))
    assign(Var, y, parent.frame())
}

tblname    %||=% "test"

getRealConnection <- function(){
  # set variables if not set yet
  serveraddr %||=% "localhost"
  user       %||=% "default"
  password   %||=% ""

  # TODO: recycle connection by using singleton?
  conn <- dbConnect(RClickhouse::clickhouse(), host=serveraddr, user=user, password=password)
  return (conn)
}

#' @export
#' @rdname tbl_lazy
simulate_clickhouse <- function(type = NULL) {
  structure(
    list(),
    class = c(type, "ClickhouseConnection", "DBITestConnection", "DBIConnection")
  )
}

writeReadTest <- function(input, result = input, types = NULL) {
  skip_on_cran()
  conn <- getRealConnection()

  dbWriteTable(conn, tblname, input, overwrite=T, field.types=types)
  afterReadWrite <- dbReadTable(conn, tblname)

  #  checks consistency of dataTypes before and after ReadWrite
  if(!is.null(types)){
    afterReadWriteType <- attr(afterReadWrite, "data.type")
    resultType <- types
    expect_equal(resultType, afterReadWriteType)
  }

  #  checks consistency of data before and after ReadWrite
  attr(afterReadWrite, "data.type") <- NULL
  attr(result, "data.type") <- NULL
  names(result) <- sapply(names(result),RClickhouse:::escapeForInternalUse,forsql=FALSE)
  expect_equal(afterReadWrite, result)

  RClickhouse::dbRemoveTable(conn,tblname)
  dbDisconnect(conn)
}
