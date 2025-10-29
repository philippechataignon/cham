#' @importFrom R6 R6Class

.onAttach <- function(libname, pkgname) {
  if (!interactive()) {
    return
  }
  packageStartupMessage(paste(
    "Package",
    pkgname,
    utils::packageVersion(pkgname)
  ))
}
