#' @title fonction tirage proba inégales
#' @export
tirage <- function(poids, cible = NULL) {
  poids[is.na(poids)] <- 0
  # si cible non spécifiée, on prend somme des poids inclusion
  if (is.null(cible)) {
    cible = as.integer(sum(poids))
  }
  if (cible > sum(poids[poids > 0])) {
    warning("Tirage : cible > nb elements avec pi > 0")
  }
  # calcul proba inclusion à partir des poids inclusion
  pik <- suppressWarnings(sampling::inclusionprobabilities(poids, cible))
  # tirage systémique avec brouillage aléatoire
  ech <- sampling::UPrandomsystematic(pik) == 1
  if (sum(ech) != cible) {
    warning("Tirage : cible non atteinte")
  }
  ech
}
