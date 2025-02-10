#' Écrit fichier parquet depuis une table duckdb
#' @param conn : connexion duckdb
#' @param name : nom de la table
#' @param filename : nom du fichier ou répertoire si partition est renseigné, par défaut `name`
#' @param dir : répertoire de sortie, éventuellement s3
#' @param partition : chaine de la forme "var1, var2" pour créer une partition, pas de partition par défaut
#' @param verbose : indique la requete SQL générée
#' @return filename
#' @export
write_duckdb_parquet <- function(conn, name = NULL, filename = NULL, dir=".", partition = NULL, verbose = FALSE)
{
  if (is.null(filename)) {
    filename = file.path(dir, paste0(name, ".parquet"))
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
