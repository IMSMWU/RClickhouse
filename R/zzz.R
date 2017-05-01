.onUnload <- function(libpath) {
  gc() # Force garbage collection of connections
}
