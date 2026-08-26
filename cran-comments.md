# cran-comments

## Summary

This release fixes the clang23 "Additional issues" failure reported by the CRAN team (deadline 2026-09-16). libc++ in LLVM 23 dropped many transitive includes, so `vendor/clickhouse-cpp/clickhouse/base/compressed.cpp` failed to compile: `std::uncaught_exceptions()` was used without a direct `#include <exception>`. The fix adds that include, mirroring the upstream clickhouse-cpp commit a7ae5b0b. No other changes.

The fix was verified locally by compiling with `-D_LIBCPP_REMOVE_TRANSITIVE_INCLUDES` (the libc++ mechanism behind the LLVM 23 change): before the fix this reproduces the reported error, after the fix the package compiles cleanly.

## Test environments

* local: macOS, R 4.5.3 (Apple clang 21, libc++, also with -D_LIBCPP_REMOVE_TRANSITIVE_INCLUDES)

## R CMD check results

0 errors | 0 warnings | 0 notes

(Locally a single NOTE about an outdated HTML Tidy binary is shown; this is a
toolchain artifact of the build machine, unrelated to the package.)
