# cran-comments

## Summary

This release fixes the ERRORs currently shown on the CRAN check results page
for RClickhouse. They were caused by the package relying on dbplyr's first
edition interface, which current dbplyr versions (>= 2.4) no longer support.
The package now declares the dbplyr second edition via a `dbplyr_edition()`
method, and the affected tests (test-agg.R, test-prefix.R) pass again.

## Test environments

* local: macOS, R 4.5.3 (with dbplyr 2.6.0, dplyr 1.2.1)

## R CMD check results

0 errors | 0 warnings | 0 notes

(In the offline build environment a single NOTE "unable to verify current
time" is shown; this is an environment artifact unrelated to the package.)
