# RClickhouse

![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg)
[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/RClickhouse)](https://cran.r-project.org/package=RClickhouse)
[![CRAN RStudio mirror downloads](http://cranlogs.r-pkg.org/badges/RClickhouse)](https://cran.r-project.org/package=RClickhouse)

## Overview

[ClickHouse &copy;](https://clickhouse.com/) is a high-performance, relational column-store database for big-data exploration and analytics scaling to petabytes of data. `RClickhouse` is a [DBI](https://dbi.r-dbi.org/)-compliant interface to it, with [dplyr](https://dplyr.tidyverse.org/)/[dbplyr](https://dbplyr.tidyverse.org/) support for auto-generated SQL.

**RClickhouse 2.0 is a ground-up rewrite.** All communication now happens through [Apache Arrow](https://arrow.apache.org/), so type conversion is handled natively by Arrow on both ends and **the package no longer contains any compiled code**. The previous versions (0.6.x), which bundled the C++ [ClickHouse client](https://github.com/ClickHouse/clickhouse-cpp) and talked to the native binary protocol, are still available on the `master` branch and on CRAN.

To cite this library, please use the BibTeX entry provided in **inst/CITATION**.

### Transports

ClickHouse can be reached over several protocols. RClickhouse 2.0 ships two Arrow-based transports, selected with the `transport` argument of `dbConnect()`:

- **`"flightsql"` (default):** the native [Arrow Flight SQL](https://clickhouse.com/docs/interfaces/arrowflight) protocol via [ADBC](https://arrow.apache.org/adbc/). It requires (a) a ClickHouse server that exposes the Arrow Flight interface (set `arrowflight_port`; the feature is recent, is not enabled by default, and is currently not offered by ClickHouse Cloud) and (b) the `adbcflightsql` driver, which is distributed via [R-multiverse](https://r-multiverse.org/) rather than CRAN (see *Installation*).
- **`"http"`:** the ClickHouse HTTP interface using the `ArrowStream` format, implemented in pure R (`httr2` + `nanoarrow`). It works against any ClickHouse server, including ClickHouse Cloud, and needs no compiled driver. Use it when Flight SQL is not available.

If you primarily want the HTTP interface, also consider [`ClickHouseHTTP`](https://github.com/patzaw/ClickHouseHTTP) ([CRAN](https://cran.r-project.org/package=ClickHouseHTTP)), a dedicated HTTP DBI backend.

## Installation

```R
install.packages("RClickhouse")
```

To use the **Flight SQL** transport, also install the ADBC driver from R-multiverse (it is not on CRAN):

```R
install.packages("adbcflightsql", repos = "https://community.r-multiverse.org")
```

Development version from GitHub:

```R
# install.packages("pak")
pak::pak("IMSMWU/RClickhouse@v2-flight-sql")
```

## Usage

### Create a DBI connection

```r
library(DBI)

# Flight SQL (default), port 9090
con <- dbConnect(RClickhouse::clickhouse(), host = "example-db.com")

# HTTP interface, port 8123
con <- dbConnect(RClickhouse::clickhouse(), host = "example-db.com",
                 transport = "http")
```

The default port follows the transport (Flight SQL `9090`, HTTP `8123`/`8443`); override it with the `port` argument if needed.

### Write data

```r
dbWriteTable(con, "mtcars", mtcars)
dbListTables(con)
dbListFields(con, "mtcars")
```

### Query with dplyr

```r
library(dplyr)
tbl(con, "mtcars") %>%
  group_by(cyl) %>%
  summarise(smpg = sum(mpg))

tbl(con, "mtcars") %>%
  filter(cyl == 8, vs == 0) %>%
  group_by(am) %>%
  summarise(mean(qsec))
```

### Query with SQL

```r
dbGetQuery(con, "SELECT vs, COUNT(*) AS n, AVG(qsec) AS avg_qsec
                 FROM mtcars GROUP BY vs")

mtcars_local <- dbReadTable(con, "mtcars")

dbDisconnect(con)
```

### Configuration files

Connection parameters can be stored in simple `key: value` files, looked up automatically (default paths: `./RClickhouse.yaml`, `~/.R/RClickhouse.yaml`, `/etc/RClickhouse.yaml`; override with `config_paths`). Recognised keys are `host`, `port`, `dbname`, `user`, `password`, `transport` and `tls`:

```yaml
host: example-db.com
transport: http
port: 8123
```

Resolution precedence: explicit `dbConnect()` arguments &gt; configuration files (earlier paths win) &gt; built-in defaults.

## Migrating from 0.6.x

Version 2.0 is a deliberate major break. The old `dbConnect()` arguments still work with deprecation warnings, but the default port and transport changed (the native protocol port `9000` is no longer supported). See `vignette("migration", package = "RClickhouse")` for details.

## Local development

A `docker-compose.yml` is provided that starts a ClickHouse server exposing both the HTTP (`8123`) and Arrow Flight SQL (`9090`) interfaces:

```sh
docker compose up -d
```

Integration tests connect to it; configure with the `RCLICKHOUSE_TEST_TRANSPORT`, `RCLICKHOUSE_TEST_HOST` and `RCLICKHOUSE_TEST_PORT` environment variables.

## Acknowledgements

Big thanks to Kirill Müller, Maxwell Peterson, Artemkin Pavel and Hannes Mühleisen, and to the Apache Arrow ADBC and nanoarrow maintainers.
