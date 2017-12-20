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
### Config File
You may use a config file that is looked up for automatic initialization of the dbConnect parameters.

To do so, create a yaml file (default ```RClickhouse.yaml```), in at least one directory (default lookup paths of parameter config_paths: ```./RClickhouse.yaml, ~/.R/RClickhouse.yaml, /etc/RClickhouse.yaml```), e.g. ```~/.R/configs/RClickhouse.yaml``` and pass a vector of the corresponding file paths to ```dbConnect``` as ```config_paths``` parameter.
In ```RClickhouse.yaml```, you may specify a variable number of parameters (```host, port, db, user, password, compression```) to be initialized using the following format (example):
```YAML
host: example-db.com
port: 1111
```
The actual initialization of the parameters of ```dbConnect``` follows a hierarchical structure with varying priorities (1 to 3, where 1 is highest):
 1. Specified input parameters when calling ```dbConnect```. If parameters are unspecified, fall back to (2)
 2. Parameters specified in ```RClickhouse.yaml```, where the level of priority depends on the position of the path in the config_path input vector (first position, highest priority etc.). If parameters are unspecified, fall back to (3).
 3. Default parameters (```host="localhost", port = 9000, db = "default", user = "default", password = "", compression = "lz4"```).


## Acknowledgements
Big thanks to Kirill Müller, Maxwell Peterson, Artemkin Pavel and Hannes Mühleisen.
