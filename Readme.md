# RClickhouse

![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg) ![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](https://img.shields.io/github/release/IMSMWU/RClickhouse.svg) [![Build Status](https://travis-ci.org/IMSMWU/RClickhouse.svg?branch=master)](https://travis-ci.org/IMSMWU/RClickhouse)
[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/RClickhouse)](https://cran.r-project.org/package=RClickhouse)
[![CRAN RStudio mirror downloads](http://cranlogs.r-pkg.org/badges/RClickhouse)](https://cran.r-project.org/package=RClickhouse)

This R package is a DBI interface for the [Yandex Clickhouse](https://clickhouse.yandex) database. It provides basic dplyr support by auto-generating SQL-commands using dbplyr and is based on the official [C++ Clickhouse Client](https://github.com/artpaul/clickhouse-cpp).

## Requirements & Installation
This package is available on CRAN, and thus installable by running:

```R
install.packages("RClickhouse")
```

You can install the latest development version directly from github using devtools like this:

```R
devtools::install_github("IMSMWU/RClickhouse")
```

## Usage
```R
# create a DBI Connection
con <- DBI::dbConnect(RClickhouse::clickhouse(), host="example-db.com")

# now you can write data to the db
DBI::dbWriteTable(con, "mtcars", mtcars)

# ... and query it using dpylr
library(dplyr)

tbl(con, "mtcars") %>% group_by(cyl) %>% summarise(smpg=sum(mpg))
```

## Acknowledgements
Big thanks to Kirill Müller, Maxwell Peterson, Artemkin Pavel and Hannes Mühleisen.
