#' RClickhouse: a DBI interface to ClickHouse over Apache Arrow
#'
#' RClickhouse provides a [DBI][DBI::DBI]-compliant interface to the
#' [ClickHouse](https://clickhouse.com/) database. All communication uses
#' Apache Arrow as the wire format, so type conversion is handled natively on
#' both ends and the package contains no compiled code.
#'
#' Two transports are available, selected with the `transport` argument of
#' [dbConnect()]:
#'
#' * `"flightsql"` (default): the native
#'   [Arrow Flight SQL](https://clickhouse.com/docs/interfaces/arrowflight)
#'   protocol via ADBC. Requires the `adbcflightsql` and `adbcdrivermanager`
#'   packages and a ClickHouse server with `arrowflight_port` enabled.
#' * `"http"`: the ClickHouse HTTP interface using the `ArrowStream` format.
#'   Pure R (`httr2` + `nanoarrow`), works against any ClickHouse server,
#'   including ClickHouse Cloud.
#'
#' @keywords internal
"_PACKAGE"

## usethis namespace: start
## usethis namespace: end
NULL
