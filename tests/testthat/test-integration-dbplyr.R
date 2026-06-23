# dbplyr backend tests against a live ClickHouse server.

test_that("group_by + summarise translates and collects", {
  skip_if_not_installed("dplyr")
  con <- local_connection()
  tbl <- tmp_table()
  on.exit(try(dbRemoveTable(con, tbl), silent = TRUE), add = TRUE)
  dbWriteTable(con, tbl, mtcars, overwrite = TRUE, row.names = FALSE)

  out <- dplyr::tbl(con, tbl) |>
    dplyr::group_by(cyl) |>
    dplyr::summarise(n = dplyr::n(), mean_mpg = mean(mpg, na.rm = TRUE)) |>
    dplyr::arrange(cyl) |>
    dplyr::collect()

  expect_equal(out$cyl, c(4, 6, 8))
  expect_equal(out$n, c(11, 7, 14))
  expect_equal(round(out$mean_mpg, 2), c(26.66, 19.74, 15.10))
})

test_that("filter + mutate with ^ translates to pow()", {
  skip_if_not_installed("dplyr")
  con <- local_connection()
  tbl <- tmp_table()
  on.exit(try(dbRemoveTable(con, tbl), silent = TRUE), add = TRUE)
  dbWriteTable(con, tbl, mtcars, overwrite = TRUE, row.names = FALSE)

  out <- dplyr::tbl(con, tbl) |>
    dplyr::filter(cyl == 8) |>
    dplyr::mutate(power = hp^2) |>
    dplyr::summarise(s = sum(power)) |>
    dplyr::collect()

  expect_equal(out$s, sum(mtcars$hp[mtcars$cyl == 8]^2))
})
