#' Compression zstd pour write_parquet
#' @export
write_parquet <- function(dt, path, quiet=F,...)
{
  if (!quiet) {
    if ("SubTreeFileSystem" %in% class(path)) {
      cat("Write s3", path$base_path, "\n")
    } else {
      cat("Write", path, "\n")
    }
  }
  arrow::write_parquet(dt, path, ..., compression = "zstd")
}
