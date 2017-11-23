# RClickhouse

![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg) ![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](https://img.shields.io/github/release/inkrement/clckhs.svg) [![Build Status](https://travis-ci.org/inkrement/clckhs.svg?branch=master)](https://travis-ci.org/inkrement/clckhs)

This R package is a DBI interface for the [Yandex Clickhouse](https://clickhouse.yandex) database. It provides basic dplyr support by auto-generating SQL-commands using dbplyr and is based on the official [C++ Clickhouse Client](https://github.com/artpaul/clickhouse-cpp).

## Requirements & Installation
A C++ compiler and for Windows Rtools are required. The latest version can be installed directly from github using devtools like this:

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
