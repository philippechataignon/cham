#' Calcul taux évolution
#' @param eff1: effectif initial
#' @param eff2: effectif final
#' @param nb_per: nombre de périodes
#' @param pct: évolution en %, FALSE par défaut
#' @param dec: nombre de décimales, par défaut toutes
#' @export
txevol <- function(eff1, eff2, nb_per, pct = F, dec = NA) {
  ret = fcase(
    nb_per == 0,
    NA_real_,
    eff1 < 0,
    NA_real_,
    eff2 < 0,
    NA_real_,
    eff1 == 0,
    0,
    eff2 == 0,
    0,
    eff1 > 0,
    (eff2 / eff1)^(1 / nb_per) - 1
  )
  if (pct) ret = ret * 100
  if (!is.na(dec)) ret = round(ret, dec)
  ret
}

#' Calcul taux évolution d'un solde
#' @param eff1: effectif initial
#' @param eff2: effectif final
#' @param solde: représente une composante de eff2 - eff1
#' @param pct: évolution en %, FALSE par défaut
#' @param dec: nombre de décimales, par défaut toutes
#' @export
txevol_solde <- function(eff1, eff2, solde, nb_per, pct = F, dec = NA) {
  ret = fcase(
    nb_per == 0,
    NA_real_,
    eff1 == 0,
    0,
    eff2 == 0,
    0,
    eff1 == eff2,
    100 * solde / (nb_per * eff1),
    eff1 != eff2,
    txevol(eff1, eff2, nb_per) * solde / (eff2 - eff1)
  )
  if (pct) ret = ret * 100
  if (!is.na(dec)) ret = round(ret, dec)
  ret
}


#' Télécharge un fichier si absent
#' @param url: URL du fichier zip
#' @param file: chemin du fichier destination, par défaut extrait de l'url
#' @param dir: chemin du fichier destination, par défaut la variable
#'   d'environnement DOWNLOAD_DIR si elle existe, sinon répertoire temporaire
#' @param force: TRUE pour forcer le téléchargement même si le fichier existe,
#'   FALSE par défaut
#' @export
download <- function(url, file, dir, force = FALSE) {
  if (missing(file)) file = basename(url)
  if (missing(dir))
    dir = fcoalesce(Sys.getenv("DOWNLOAD_DIR", unset = NA), tempdir())
  path = file.path(dir, file)
  if (force || !file.exists(path)) {
    ret = download.file(url, path)
  } else {
    cat(path, "present", "\n")
  }
  path
}


#' Télécharge et décompresse un csv zippé
#' @param url: URL du fichier zip
#' @param keep: TRUE pour conserver le zip téléchargé, FALSE par défaut
#' @export
downzip <- function(url, keep = F) {
  zipfile = tempfile()
  download.file(url = url, destfile = zipfile, method = "curl")
  biggest = zip::zip_list(zipfile) |>
    arrange(compressed_size) |>
    tail(1) |>
    pull(filename)
  ret = fread(cmd = paste("unzip -p", zipfile, biggest))
  if (!keep) unlink(zipfile)
  ret
}

#' Récupère URL du fichier données melodi depuis un id source
#' @param id: id du jeu de données, exemple DS_POPULATIONS_REFERENCE
#' @export
get_access_url <- function(id) {
  url = paste0("https://api.insee.fr/melodi/catalog/", id)
  rep = httr::GET(url, httr::accept_json()) |>
    httr::content()
  rep$product[[1]]$accessURL
}

#' Supprime les variables commençant par . dans un data.table
#' @export
drop_temp <- function(DT) {
  set(DT, j = grep("^\\.", colnames(DT)), value = NULL)
  invisible(DT)
}

#' Récupère le timestamp actuel en secondes
#' @export
get_time <- function() {
  proc.time()["elapsed"]
}

#' Indique le site actuel
#' @return Renvoit une des valeurs suivantes: "ls3", "ssp", "aus", "pc"
#' @export
get_site <- function() {
  site = Sys.getenv("SITE")
  user = Sys.getenv("USER")
  if (site == "ssp") {
    ret = "ssp"
  } else if (site == "ls3" || user == "onyxia") {
    ret = "ls3"
  } else if (site == "aus" || site == "") {
    ret = "aus"
  } else {
    ret = "pc"
  }
  ret
}

#' Renvoie une liste nommée à partir d'un pattern
#' @param l : vecteur character
#' @param pattern : pattern glue où "{x}" est remplacé par les valeurs de l
#' @return Liste d'éléments nommés avec le pattern résolu pour chauqe valeur
#' @examples
#' extend(c("a", "b", "c"), "test{x}")
#' @export
extend <- function(l, pattern) {
  ret = lapply(
    l,
    function(x) {
      gsub('{x}', x, pattern, fixed = T)
    }
  )
  names(ret) <- l
  ret
}
