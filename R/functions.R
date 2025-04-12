#' Calcul taux évolution
#' @export
txevol <- function(pop1, pop2, nb_an, dec = 6)
{
  fcase(
        pop1 < 0, NA_real_,
        pop2 < 0, NA_real_,
        pop1 == 0, 0,
        pop2 == 0, 0,
        pop1 > 0, round(((pop2/pop1) ^ (1/nb_an) - 1) * 100, dec)
  )
}

#' Calcul taux évolution d'un solde
#' @export
txevol_solde <- function(pop1, pop2, evol, nb_an, dec = 6)
{
  round(
    fcase(
      nb_an * pop1 == 0, 0,
      pop1 == pop2, 100 * evol / (nb_an * pop1),
      pop1 != pop2, txevol(pop1, pop2, nb_an) * evol / (pop2 - pop1)
    ),
  dec)
}


#' Télécharge un fichier si absent
#' @export
download <- function(url, file, dir, force = FALSE)
{
  if (missing(file))
    file = basename(url)
  if (missing(dir))
    dir = fcoalesce(Sys.getenv("DOWNLOAD_DIR", unset=NA), tempdir())
  path = file.path(dir, file)
  if (force || !file.exists(path)) {
    ret = download.file(url, path)
  } else {
    cat(path, "present", "\n")
  }
  path
}

#' Supprime les variables commençant par . dans un data.table
#' @export
drop_temp <- function(DT)
{
  set(DT, j = grep("^\\.", colnames(DT)), value = NULL)
  invisible(DT)
}

#' Récupère le timestamp actuel en secondes
#' @export
get_time <- function()
{
  proc.time()["elapsed"]
}
