#' Écrit fichier parquet depuis une table duckdb
#' @param conn : connexion duckdb
#' @param name : nom de la table
#' @param filename : nom du fichier ou répertoire si partition est renseigné, par défaut `name`
#' @param dir : répertoire de sortie, éventuellement s3
#' @param partition : chaine de la forme "var1, var2" pour créer une partition, pas de partition par défaut
#' @param verbose : indique la requete SQL générée
#' @return nom du fichier/répertoire
#' @examples
#' s3expl = "s3://insee/sern-div-exploitations-statistiques-rp/ear_rp"
#' # Fichier simple ear_fam.parquet
#' outfile = write_duckdb_parquet(conn = conn, name="ear_fam", dir = s3expl)
#' # Fichier partitionné dans répertoire test_fam
#' outfile = write_duckdb_parquet(conn, name="ear_fam",  dir = s3expl,
#'   filename="test_fam", partition="c_annee_col", verbose=T)
#' @export
write_duckdb_parquet <- function(conn, name = NULL, filename = NULL, dir=".", partition = NULL, verbose = FALSE)
{
  if (is.null(filename)) {
    filename = file.path(dir, paste0(name, ".parquet"))
  } else {
    filename = file.path(dir, filename)
  }
  if (is.null(partition)) {
    partition_str = ""
  } else {
    partition_str = glue::glue(", PARTITION_BY ({partition}), OVERWRITE_OR_IGNORE")
  }
  cmd = glue::glue(
    "COPY {name} TO '{filename}'
    (FORMAT 'parquet', COMPRESSION 'zstd' {partition_str})"
  )
  if (verbose)
    cat(cmd, "\n")
  dbExecute(conn, cmd)
  filename
}
