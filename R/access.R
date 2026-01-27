#' Accès fichier geo portable
#' @param path Chemin du fichier geo
#' @export
geo <- function(path)
{
  if (site %in% c("ls3")) {
    ret = file.path(s3expl, "geoparquet", path)
  } else if (site %in% c("ssp")) {
    ret = file.path(s3perso, "geoparquet", path)
  } else if (site %in% c("aus")) {
    ret = file.path("X:","HAB-MaD-SeRN", "geoparquet", path)
  } else {
    dir = file.path(Sys.getenv("HOME"), "work", "geoparquet")
    dir.create(dir, recursive = T, showWarnings = F)
    ret = file.path(dir, path)
  }
  ret
}

#' Accès fichier mad insee
#' @param path Chemin du fichier mis à disposition
#' @export
gen <- function(path)
{
  if (site %in% c("ls3", "ssp")) {
    ret = file.path("s3://public/gen-mad", path)
  } else if (site == "aus") {
    ret = file.path("W:", path)
  } else {
    ret = file.path("~/work/gen", path)
  }
  ret
}

access_nom <- function(path, nom, s3 = TRUE)
{
  if (site %in% c("ls3", "ssp") && s3) {
    root = s3perso
  } else {
    root = "~/work"
  }
  file.path(root, nom, path)
}

#' Accès fichiers de travail
#' @param path Chemin du fichier work
#' @export
store <- function(path, s3 = TRUE)
{
  access_nom(path, "data", s3)
}

#' Accès fichiers permanents
#' @param path Chemin du fichier store
#' @export
work <- function(path, s3 = TRUE)
{
  access_nom(path, "work", s3)
}

#' Accès fichier référence
#' @param path Chemin du fichier référence
#' @export
ref <- function(path, s3 = TRUE)
{
  file.path(Sys.getenv("HOME"), "work", "data_ref", path)
}
