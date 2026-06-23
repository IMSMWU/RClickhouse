.onUnload <- function(libpath) {
  # Undo the optional dbplyr case-sensitivity patch if it was applied.
  if (!is.null(.ch_dbplyr$original)) {
    try(fix_dbplyr(), silent = TRUE)
  }
}
