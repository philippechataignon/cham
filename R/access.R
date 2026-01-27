if (site == "ls3") {
  s3perso <- paste0("s3://travail/user-", Sys.getenv("IDEP"))
} else if (site == "ssp") {
  s3perso <- paste0("s3://", Sys.getenv("IDEP"))
} else {
  s3perso <- "~/work"
}

if (site == "ls3") {
  s3expl <- "s3://insee/sern-div-exploitations-statistiques-rp"
} else if (site == "ssp") {
  s3expl <- paste0("s3://", Sys.getenv("IDEP"), "/exploitation")
} else {
  s3expl <- "~/work/insee"
}

#' Bucket perso
#' @name s3perso
#' @export
s3perso

#' Bucket exploitations statistiques
#' @name s3expl
#' @export
s3expl

#' Accès perso
#' @export
perso <- function(..., .local = FALSE)
{
  if (.local)
    file.path("~", "work", ...)
  else
    file.path(s3perso, ...)
}

#' Accès expl
#' @export
expl <- function(...)
{
  file.path(s3expl, ...)
}


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

#' Accès fichiers permanents
#' @param path Chemin du fichier store
#' @export
work <- function(path)
{
  # warning("Deprecated: utiliser `perso('work', path)` à la place")
  perso("work", path)
}

#' Accès fichier référence
#' @param path Chemin du fichier référence
#' @export
ref <- function(path, s3 = TRUE)
{
  file.path(Sys.getenv("HOME"), "work", "data_ref", path)
}
