#' Crée un filesystem s3 avec les variables environnement
#' @export
s3createfs <- function()
{
  arrow::S3FileSystem$create(
    access_key = Sys.getenv('AWS_ACCESS_KEY_ID'),
    secret_key = Sys.getenv('AWS_SECRET_ACCESS_KEY'),
    session_token = Sys.getenv('AWS_SESSION_TOKEN'),
    endpoint_override=Sys.getenv('AWS_S3_ENDPOINT')
  )
}

#' Compression zstd pour write_parquet
#' @export
write_parquet <- function(...)
{
  arrow::write_parquet(..., compression = "zstd")
}

#' Renvoit une chaine s3 depuis un chemin
#' @param path : chemin
#' @return chaîne chemin
#' @export
s3path <- function(path, bucket, prefix)
{
  if (missing(bucket))
    bucket = Sys.getenv("BUCKET")
  if (missing(prefix))
    prefix = Sys.getenv("PREFIX")
  if (nchar(prefix) == 0) {
    ret = file.path(bucket, path)
  } else {
    ret = file.path(bucket, prefix, path)
  }
  ret
}

#' Copie locale d'un chemin s3
#' @param path : chemin
#' @export
s3copy <- function(path, fs, dir="/tmp", ...)
{
  if (missing(fs))
    fs = s3fs
  outfile = file.path(dir, basename(path))
  writeBin(as.raw(fs$OpenInputFile(s3path(path, ...))$Read()), outfile)
  outfile
}

#' Renvoit un SubFileSystemTree
#' @param path : chemin
#' @note Peut être utilisé avec read_parquet, write_parquet
#'       et read_csv_arrow
#' @export
s3file <- function(path, fs, ...)
{
  if (missing(fs))
    fs = s3fs
  fs$path(s3path(path, ...))
}
