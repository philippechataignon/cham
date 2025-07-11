#' Arrondit un vecteur numérique sur une cible entière
#' @param x: vecteur à traiter contenant uniquement des valeurs positives
#' @param p: (optionnel) vecteur de poids, sinon poids = 1
#' @param target: (optionnel) entier cible pour la somme pondérée de x,
#' par défaut somme pondérée de x
#' @param na.rm: (optionnel) supprime les valeurs manquantes si TRUE, FALSE par défaut
#' @export

arrondi_entier <- function(x, p = NULL, method=c("abs", "max", "min"), target = NULL, na.rm = FALSE)
{
  method = match.arg(method)
  wp = p
  if (is.null(p)) {
    wp = 1L
    method = "max"
  }
  if (na.rm) {
    x[is.na(x)] <- 0
    wp[is.na(wp)] <- 0
  }
  if (any(is.na(x)))
    stop("x ne doit pas contenir de valeurs manquantes. na.rm=T pour les remplacer par 0")
  if (any(is.na(wp)))
    stop("p ne doit pas contenir de valeurs manquantes. na.rm=T pour les remplacer par 0")
  if (any(x < 0))
    stop("x ne doit contenir que des valeurs positives")
  if (!is.null(target)) {
    x <- x / sum(x) * target / wp
  }
  if (length(x) == 0 | sum(x) == 0) {
    xe = vector("integer", length(x))
  } else {
    wp = as.integer(wp)
    xe = as.integer(trunc(x))
    xf = (x - trunc(x)) * wp
    o <- order(xf, xe, decreasing = T)
    n = as.integer(round(sum(xf)))
    if (n != 0) {
      if (is.null(p)) {
        cs = 1:length(x)
      } else {
        cs = cumsum(p[o])
      }
      if (method == "abs") {
        nm = which.min(abs(cs - n))
      } else if (method == "max") {
        nm = match(TRUE, cs >= n)
      } else if (method == "min") {
        nm = max(which(cs <= n))
      }
      ind = o[1:nm]
      xe[ind] = xe[ind] + 1L
    }
  }
  xe
}
