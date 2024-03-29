# Generated by using Rcpp::compileAttributes() -> do not edit by hand
# Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

fetch <- function(res, n) {
    .Call(`_RClickhouse_fetch`, res, n)
}

clearResult <- function(res) {
    invisible(.Call(`_RClickhouse_clearResult`, res))
}

hasCompleted <- function(res) {
    .Call(`_RClickhouse_hasCompleted`, res)
}

getRowCount <- function(res) {
    .Call(`_RClickhouse_getRowCount`, res)
}

getRowsAffected <- function(res) {
    .Call(`_RClickhouse_getRowsAffected`, res)
}

getStatement <- function(res) {
    .Call(`_RClickhouse_getStatement`, res)
}

resultTypes <- function(res) {
    .Call(`_RClickhouse_resultTypes`, res)
}

connect <- function(host, port, db, user, password, compression) {
    .Call(`_RClickhouse_connect`, host, port, db, user, password, compression)
}

disconnect <- function(conn) {
    invisible(.Call(`_RClickhouse_disconnect`, conn))
}

select <- function(conn, query) {
    .Call(`_RClickhouse_select`, conn, query)
}

insert <- function(conn, tableName, df) {
    invisible(.Call(`_RClickhouse_insert`, conn, tableName, df))
}

validPtr <- function(ptr) {
    .Call(`_RClickhouse_validPtr`, ptr)
}

# Register entry points for exported C++ functions
methods::setLoadAction(function(ns) {
    .Call(`_RClickhouse_RcppExport_registerCCallable`)
})
