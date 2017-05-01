# Clckhs

![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg) ![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](https://img.shields.io/github/release/inkrement/clckhs.svg) [![Build Status](https://travis-ci.org/inkrement/clckhs.svg?branch=master)](https://travis-ci.org/inkrement/clckhs)

This R package is a DBI interface and dplyr SQL-backend for the [clickhouse database](https://clickhouse.yandex). It is based on the [C++ Clickhouse Client](https://github.com/artpaul/clickhouse-cpp) and uses compression for data transfer.

## Requirements & Installatiom
You need a C++ compiler and for Windows Rtools is required. This package is currently not available on CRAN so you have to install it using devtools:

```R
devtools::install_github("inkrement/clckhs@cpp")
```

## Usage
```R
# create a DBI Connection
con <- DBI::dbConnect(clckhs::clickhouse(),host="example-db.com")

# now you can write data to the db
DBI::dbWriteTable(con, "mtcars", mtcars)

# ... and query it using dpylr
library(dplyr)

tbl(con, "mtcars") %>% group_by(cyl) %>% summarise(smpg=sum(mpg))
```

## Acknowledgements
Big thanks to Kirill Müller, Maxwell Peterson, Artemkin Pavel and Hannes Mühleisen.
