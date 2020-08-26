context("escape")
# tests the various layers of RCH for correct escaping

library(DBI, warn.conflicts=F)
library(dplyr, warn.conflicts=F)

source("utils.R")

# tests if IDENTIFIERS are correctly encoded: creation of tables
noSpaceDoubleQuotes <- data.frame(
  "capitals"=c("Vienna","Washington D.C.", "Paris", "London"),
  "best_sights"=c("Viennese Opera","Lincoln Memorial", "Eiffel Tower", "London Bridge"),
  "Col2_no_space"=1:4,
  stringsAsFactors=FALSE,
  check.names=FALSE
)
test_that("creating noSpaceColumns IDENTIFIED with doubleQuotes", {
  writeReadTest(as.data.frame(noSpaceDoubleQuotes))
})


# currently failing
# withGapDoubleQuotes <- data.frame(
#   "capitals"=c("Vienna","Washington D.C.", "Paris", "London"),
#   "best sights"=c("Viennese Opera","Lincoln Memorial", "Eiffel Tower", "London Bridge"),
#   "Col2 with space"=1:4,
#   stringsAsFactors=FALSE,
#   check.names=FALSE
# )
# test_that("reading & writing array columns", {
#   writeReadTest(as.data.frame(withGapDoubleQuotes))
# })



# TODO
# extensive cases for Tablenames with and without spaces in them
# --> and the insertion of values
# --> and both

