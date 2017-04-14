# Clckhs (Alpha)

![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](http://www.repostatus.org/badges/latest/active.svg) ![Project Status: Active - The project has reached a stable, usable state and is being actively developed.](https://img.shields.io/github/release/inkrement/clckhs.svg)

This R package is a basic DBI interface and dplyr SQL-backend for the clickhouse database. It is based on the [C++ Clickhouse Client](https://github.com/artpaul/clickhouse-cpp).

It is currently not available on CRAN so you have to install it using devtools:
```R
devtools:install_github("inkrement/clckhs@cpp")
```

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



## Acknowledgements
Big thanks to Kirill Müller, Maxwell Peterson, Artemkin Pavel and Hannes Mühleisen.
