#' Permet de transformer une table duckdb en sf
#' @export
tbl2sf <- function(
    table,
    crs
)
{
    table |>
      arrow::to_arrow() |>
      sf::st_as_sf(crs = crs) |>
      tibble::as_tibble() |>
      sf::st_as_sf(crs = crs)
}

#' Permet de lire un fichier geoparquet en sf
#' @export
read_geoparquet <- function(
  path,
  crs=NULL,
  transform = F,
  xy = F,
  verbose=F
)
{
  is_s3 = grepl("^s3://", path)
  if (!is_s3 & !file.exists(path))
    stop("Le fichier ", path, " n'existe pas")
  if (transform && is.null(crs))
    stop("La paramètre 'crs' est obligatoire avec 'transform'")
  require(geoarrow)
  conn_temp = get_conn(new = T)
  meta = get_geometadata(path, verbose=verbose)
  geom = meta$primary_column
  crs_src = paste(
    meta$columns$geometry$crs$id$authority,
    meta$columns$geometry$crs$id$code,
    sep=":"
  )
  if (length(crs_src) == 0)
    crs_src = "EPSG:4326"
  if (is.null(crs))
    crs_dest = crs_src
  else if (is.numeric(crs))
    crs_dest = paste0("EPSG:", crs)
  else
    crs_dest = crs
  if (transform) {
    geom = paste0("st_transform(", geom, ", '", crs_src, "', '", crs_dest, "',  always_xy := ", xy, ")")
    q = paste0(
      "SELECT * REPLACE (", geom, " as geometry)
      FROM read_parquet('", path, "')"
    )
    if (verbose)
      cat(q, "\n")
    ret = tbl(conn_temp, sql(q))
  } else {
    ret = tbl_pqt(conn_temp, path)
  }
  ret = tbl2sf(ret, crs=crs_dest)
  DBI::dbDisconnect(conn_temp)
  ret
}

#' Permet d'écrire un fichier geoparquet en sf
#' @export
write_geoparquet <- function (sf, path, verbose=F)
{
  if (!inherits(sf, "sf")) {
    stop("Must be sf data format")
  }
  if (missing(path)) {
    stop("Missing output file")
  }
  geom_cols <- lapply(sf, function(i) inherits(i, "sfc"))
  geom_cols <- names(which(geom_cols==TRUE))
  col_meta <- list()

  for(col in geom_cols){
    col_meta[[col]] <- list(
      crs = jsonlite::fromJSON(sf::st_as_text(sf::st_crs(sf[[col]]), projjson=T)),
      encoding = "WKB",
      geometry_types = list()
    )
  }
  geo_metadata <- list(
    primary_column = attr(sf, "sf_column"),
    version = "1.1.0",
    columns = col_meta
  )
  geo_metadata = jsonlite::toJSON(geo_metadata, auto_unbox=T)
  if (verbose)
    cat(jsonlite::prettify(geo_metadata), "\n")

  for(col in geom_cols){
    obj_geo <- sf::st_as_binary(sf[[col]])
    attr(obj_geo, "class") <- c("arrow_binary", "vctrs_vctr", attr(obj_geo, "class"), "list")
    sf[[col]] <- obj_geo
  }
  tbl <- arrow::Table$create(sf)
  tbl$metadata[["geo"]] <- geo_metadata
  arrow::write_parquet(tbl, sink = path, compression = "zstd")
  invisible(sf)
}

#' Récupère le polygone associé à un hex_id
#' @export
get_h3map <- function(hex_id)
{
  rbindlist(cellToBoundary(hex_id), idcol="id") |>
    sf::sf_polygon("lat", "lng", polygon_id="id") |>
    sf::st_sf(crs=4326)
}

#' Récupère les métadonnées géographiques
#' @export
get_geometadata <- function(path, valid=T, verbose=T)
{
  meta = arrow::open_dataset(path)$metadata
  if (!"geo" %in% names(meta)) {
    stop("No 'geo' entry in file metadata")
  }
  if (verbose)
    cat(jsonlite::prettify(meta), "\n")
  meta = jsonlite::fromJSON(meta$geo)
  if (valid) {
    req_names <- c("version", "primary_column", "columns")
    for(n in req_names){
      if(!n %in% names(meta)){
        stop(paste0("Required name: '", n, "' not found in geo metadata"))
      }
    }
    if (meta$version < "1") {
      stop("Minimal version is 1.0.0. Found ", meta$version)
    }
    req_geo_names <- c("geometry_types", "encoding")
    for (c in names(meta$columns)){
      geo_col <- meta$columns[[c]]
      for(ng in req_geo_names){
        if(geo_col[["encoding"]] != "WKB"){
          stop("Only well-known binary (WKB) encoding is currently supported.")
        }
        if(!ng %in% names(geo_col)){
          stop(paste0("Required 'geo' metadata item '", ng, "' not found in ", c))
        }
      }
    }
  }
  meta
}
