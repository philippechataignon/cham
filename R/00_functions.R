#' Calcul taux évolution
#' @param eff1 effectif initial
#' @param eff2 effectif final
#' @param nb_per nombre de périodes
#' @param pct évolution en %, FALSE par défaut
#' @param dec nombre de décimales, par défaut toutes
#' @export
txevol <- function(eff1, eff2, nb_per, pct = F, dec = NA) {
  ret = case_when(
    nb_per == 0 ~ NA_real_,
    eff1 < 0 ~ NA_real_,
    eff2 < 0 ~ NA_real_,
    eff1 == 0 ~ 0,
    eff2 == 0 ~ 0,
    eff1 > 0 ~ (eff2 / eff1)^(1 / nb_per) - 1
  )
  if (pct) {
    ret = ret * 100
  }
  if (!is.na(dec)) {
    ret = round(ret, dec)
  }
  ret
}

#' Calcul taux évolution d'un solde
#' @param eff1 effectif initial
#' @param eff2 effectif final
#' @param solde représente une composante de eff2 - eff1
#' @param pct évolution en %, FALSE par défaut
#' @param dec nombre de décimales, par défaut toutes
#' @export
txevol_solde <- function(eff1, eff2, solde, nb_per, pct = F, dec = NA) {
  ret = case_when(
    nb_per == 0 ~ NA_real_,
    eff1 == 0 ~ 0,
    eff2 == 0 ~ 0,
    eff1 == eff2 ~ solde / (nb_per * eff1),
    eff1 != eff2 ~ txevol(eff1, eff2, nb_per) * solde / (eff2 - eff1)
  )
  if (pct) {
    ret = ret * 100
  }
  if (!is.na(dec)) {
    ret = round(ret, dec)
  }
  ret
}

#' Télécharge un fichier si absent
#' @param url URL du fichier
#' @param file Chemin du fichier destination, par défaut extrait de l'url
#' @param dir Chemin du fichier destination, par défaut répertoire courant
#' @param force TRUE pour forcer le téléchargement même si le fichier existe, FALSE par défaut
#' @param verbose TRUE pour afficher davantage d'information, FALSE par défaut
#' @export
download <- function(url, file, dir = tempdir(), force = FALSE, verbose = FALSE)
{
  if (missing(file)) {
    file = basename(url)
  }
  path = file.path(dir, file)
  if (force || !file.exists(path)) {
    ret = download.file(url, path, mode="wb")
      if (verbose)
        cat(path, "downloaded\n")
  } else {
    if (verbose)
      cat(path, "already present\n")
  }
  path
}

#' Télécharge et décompresse une fichier compressé
#' @param url URL du fichier
#' @param file chemin du fichier destination, par défaut extrait de l'url
#' @param dir chemin du fichier destination, par défaut répertoire courant
#' @param force TRUE pour forcer le téléchargement même si le fichier existe, FALSE par défaut
#' @param verbose TRUE pour afficher davantage d'information, FALSE par défaut
#' @export
download_archive <- function (url, file = NULL, dir = NULL, force = FALSE, verbose = FALSE)
{
  if (is.null(dir)) {
    dirout = tempdir()
  } else {
    dirout = dir
  }
  archfile = download(url, dir = dir, force = force)
  file_list = archive::archive(archfile) |>
    arrange(size)
  if (verbose)
    print(file_list)
  if (is.null(file))
    extract_file = dplyr::pull(tail(file_list,  1), path)
  else
    extract_file = file
  outfile = archive::archive_extract(archfile, dir=dirout, file=extract_file)
  if (verbose)
    cat(archfile, "downloaded, extract", outfile, "in", dirout, "\n")
  file.path(dirout, outfile)
}

#' Récupère URL du fichier données melodi depuis un id source
#' @param id id du jeu de données, exemple DS_POPULATIONS_REFERENCE
#' @export
get_access_url <- function(id)
{
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

#' Nom du site
#'
#' @export
site = get_site()

#' Fonction lapply avec utilisation des noms de la liste nommée
#' @param l liste character
#' @param f fonction appliquée à chaque élément de la liste
#' @return Liste d'éléments nommés avec le résultat de la fonction
#' @export
lapplyn <- function(l, f) {
  ret = lapply(l, f)
  names(ret) = l
  ret
}

#' Renvoie une liste nommée à partir d'un pattern
#' @param l vecteur character
#' @param pattern pattern glue où "{x}" est remplacé par les valeurs de l
#' @return Liste d'éléments nommés avec le pattern résolu pour chauqe valeur
#' @examples
#' extend(c("a", "b", "c"), "test{x}")
#' @export
extend <- function(l, pattern) {
  ret = lapplyn(
    l,
    function(x) {
      gsub('{x}', x, pattern, fixed = T)
    }
  )
  ret
}

#' Renvoie TRUE si écart dans la norme
#' @param v1 valeur initiale
#' @param v2 valeur finale
#' @param abs écart absolu maximal, 0 par défaut
#' @param rel écart relatif maximal, 0 par défaut
#' @return Renvoie TRUE si écart dans la norme, FALSE sinon
#' @examples
#' # OK si ecart < 5% ou <= 10
#' ecart(c(100, 100, 100), c(105, 115, 80), 10, 0.05)
#' # [1]  TRUE FALSE FALSE
#' @export
ecart <- function(v1, v2, abs = 0, rel = 0) {
  abs(v2 - v1) < pmax(abs, rel * pmin(v1, v2))
}

#' Compte le nombre de bits par éléement d'un vecteur entier
#' @param n Vecteur d'entiers
#' @return Nombre de '1' dans l'expression binaire
#' @examples count_bits(1:16)
#' @export
count_bits <- function(n)
{
  count = vector("integer", length(n))
  while(any(n) > 0) {
    count[n>0] = count[n>0] + 1
    n[n>0] = bitwAnd(n[n>0], n[n>0]-1)
  }
  count
}


#' Crée une valeur de 'grouping' par facililter l'utilsation des fonctions 'cube'
#' @param x Vecteur de booleans avec TRUE pour les dimensions présentes
#' @return Valeur de groupe correspondant
#' @examples
#' # Croisement région x statut occupation
#' group_num(c(reg=T, dep=F, densaav=F, maisappt=F, achev=F, typc=F, stoc=T, surf=F))
#' # 125 = 255 - (128 + 2)
#' @export
group_num <- function(x)
{
  ret = 0
  for (i in 1:length(x)) {
    # On décale à gauche
    ret = bitwShiftL(ret, 1)
    if (x[i]) {
      ret = bitwOr(ret, 1L)
      # et on met le dernier bit à 1 si TRUE
    }
  }
  2 ** length(x) - ret - 1
}
