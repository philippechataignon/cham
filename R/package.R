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
