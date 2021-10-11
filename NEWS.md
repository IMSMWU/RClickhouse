RClickhouse v0.6.0
==============

 * implements DBI functions dbCreateTable and dbAppendTable
 * adds support for table names with spaces or special characters
 * renames db parameter in dbConnect to dbname
 * adds test cases
 * fix: properly nests Date-Types in Nullable
 * fix: join on the same key when using dplyr is now possible
 * fix: removed .Internal calls when setting Encoding


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
