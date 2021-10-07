RClickhouse v0.6.0
==============

 * properly nest Date-Types in Nullable
 * rename db parameter in dbConnect to dbname
 * fix quotation of identifiers
 * add test cases


RClickhouse v0.5.2
==============

 * fix windows connection
 * replace yaml dependencies by R code (reduce dependencies)
 * update clickhouse-cpp dependencies


RClickhouse v0.5.1
==============

 * fixes solaris build


RClickhouse v0.5
==============

 * add missing stdexcept headers (gcc-10 compatibility)
 * enhanced dplyr join support (thanks to Oliver Flasch)
 * extended documentation (thanks to Malte Kyhos & Alicja Grzadziel)


RClickhouse 0.4
==============

 * Enhanced sql-translation (e.g., custom if null and is null checks)
 * Packrat was removed from repository
 * UUID-support
 * The package now supports config-files
