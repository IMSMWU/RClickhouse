# Clckhs (Alpha)

This R package is a basic DBI interface and dplyr SQL-backend for the clickhouse database. It is based on the [C++ Clickhouse Client](https://github.com/artpaul/clickhouse-cpp).

```R
conn <- dbConnect(clckhs::clickhouse(), host="localhost")
```

## Next Steps

 - enhance dplyr support
 - add usage information


## Dev Info
 * Build and Reload Package:  `Ctrl + Shift + B`
 * Check Package:             `Ctrl + Shift + E`
 * Test Package:              `Ctrl + Shift + T`

Generate Namespace: `devtools::document()`
To use latest dplyr-package: ` devtools::install_github("hadley/dplyr")`


# Other dplyr sql-backends:
 * https://github.com/snowflakedb/dplyr-snowflakedb
 * https://gist.github.com/piccolbo/3d8ac40291f4eaee644b
 
## Acknowledgements
Big thanks to Kirill Müller, Maxwell Peterson, Artemkin Pavel and Hannes Mühleisen.
