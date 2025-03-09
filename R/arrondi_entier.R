#' Arrondit un vecteur numérique sur une cible entière
#' @param x: vecteur à traiter contenant uniquement des valeurs positives
#' @param target: entier cible pour la somme de x, par défaut arrondi de la somme de x
#' @param na.rm: supprime les valeurs manquantes si TRUE, FALSE par défaut
#' @param verbose: si TRUE, affiche des informations sur le calcul
#' @export
arrondi_entier <- function(x, target = NULL, na.rm = F, verbose = F)
{
  if (!na.rm && any(is.na(x)))
    stop("x ne doit pas contenir de NA. ",
         "Utiliser na.rm = T pour ne pas tenir ",
         "compte des valeurs manquantes")
  if (any(x < 0))
    stop("x ne doit contenir que des valeurs positives")
  if (is.null(target)) {
    target <- round(sum(x, na.rm = na.rm))
  }
  target <- as.integer(target)
  x <- x / sum(x) * target
  xf <- as.integer(floor(x))
  xs = sum(xf, na.rm = na.rm)
  if (verbose) {
    cat("target:", target, "\n")
    cat("target - xs:", target - xs, "\n")
  }
  # ajoute 1 aux indices correspondant aux (target - xs) parties décimales
  # les plus importantes
  if (xs < target) {
    xd <- order(x - xf, decreasing = T)[1:(target - xs)]
    xf[xd] <- xf[xd] + 1L
  }
  if (sum(xf) != target) {
    warning("Ecart: sum(xf)=", sum(xf), " - target=", target)
  }
  xf
}
