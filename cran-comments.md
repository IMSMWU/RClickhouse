## Release summary

RClickhouse 2.0.0 is a major, ground-up rewrite. The package now communicates
with ClickHouse through Apache Arrow (Arrow Flight SQL via ADBC, or the HTTP
interface with the ArrowStream format) and **no longer contains any compiled
code** (the previously bundled C++ ClickHouse client has been removed, and
`NeedsCompilation` is now `no`).

## Test environments

* local macOS, R 4.5.3
* GitHub Actions: ubuntu-latest (R release, R devel), macOS, Windows (R release)

## R CMD check results

0 errors | 0 warnings | 1 note

* The note concerns the `Additional_repositories` field, which points to
  R-multiverse (https://community.r-multiverse.org). This is required for the
  optional, Suggested package `adbcflightsql` — the Apache Arrow ADBC Flight SQL
  driver — which is distributed via R-multiverse and is not available on CRAN.
  The package is fully functional without it: the default examples and the HTTP
  transport depend only on CRAN packages, and all uses of `adbcflightsql` are
  guarded with `requireNamespace()` / `rlang::check_installed()`.

## Reverse dependencies

None.
