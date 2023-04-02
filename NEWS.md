RClickhouse v0.6.7
==============

 * explicit void for c functions without type
 * align dbplyr function argument names with prototypes
 * drop support for <R 3.6 such that C/C++11 is supported by default
 * use bibentry for CITATION
 * remove LazyData section from DESCRIPTION
 * use vector instead of personList in CITATION

RClickhouse v0.6.6
==============

 * updated to the latest dbplyr interface
 * fixed C/C++ compilation warnings
 * cleaned up the code base
 * updated dependencies (DBI, dplyr)


RClickhouse v0.6.5
==============

 * fix: now properly nests ColumnDateTime for Nullables
 
RClickhouse v0.6.4
==============

 * declared cli as import

RClickhouse v0.6.3
==============

 * reduced title length to less than 65 characters
 * removed unsupported aggregate function for translater
 * removed warning suppression
 * augmented Docs for dbplyr utils-functions
 * transitioned from deprecated data_frame() to tibble()

RClickhouse v0.6.2
==============

 * updated authorship for 'lz4'

RClickhouse v0.6.1
==============

 * updated ownership of copyright for 'lz4'
 * added gtest vendor files to .Rbuildignore

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
