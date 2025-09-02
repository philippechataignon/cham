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
      as_tibble() |>
      sf::st_as_sf(crs = crs)
}

#' Permet de lire un fichier geoparquet en sf
#' @export
read_geoparquet <- function(
  file,
  crs=NULL,
  method = c("duckarrow", "st_read", "geoarrow"),
  convert = F,
  flip = F,
  verbose=F
)
{
  method = match.arg(method)
  is_s3 = grepl("^s3://", file)
  if (!is_s3 & !file.exists(file))
    stop("Le fichier ", file, " n'existe pas")
  if (method == "geoarrow") {
    if (convert | is_s3)
      stop("Pas de conversion possible avec la méthode 'geoarrow'")
    require(geoarrow)
    if (is.null(crs))
      crs = "OGC:CRS84"
    open_dataset(file) |>
      sf::st_as_sf(crs=crs) |>
      as_tibble() |>
      sf::st_as_sf(crs=crs) -> ret
  } else {
    conn = get_conn()
    meta = DBI::dbGetQuery(
      conn,
      paste0("
      SELECT decode(value)
      FROM parquet_kv_metadata('", file, "')
      WHERE key='geo'
      ")
    )[1,]
    if (verbose)
      cat(jsonlite::prettify(meta), "\n")
    meta = jsonlite::fromJSON(meta)
    geom = meta$primary_column
    crs_src = paste(
      meta$columns$geometry$crs$id$authority,
      meta$columns$geometry$crs$id$code,
      sep=":"
    )
    if (length(crs_src) == 0)
      crs_src = "OGC:CRS84"
    if (is.null(crs))
      crs_dest = crs_src
    else if (is.numeric(crs))
      crs_dest = paste0("EPSG:", crs)
    else
      crs_dest = crs
    if (method == "st_read" || (method == "duckarrow" && convert)) {
      if (convert) {
        if (flip)
        geom = paste0("st_transform(", geom, ", '", crs_src, "', '", crs_dest, "',  xy := false)")
      else
        geom = paste0("st_transform(", geom, ", '", crs_src, "', '", crs_dest, "',  xy := true)")
      }
      sql_geom = paste0("st_aswkb(", geom, ")")
      q = paste0(
        "SELECT * REPLACE (", sql_geom, " as geometry)
        FROM read_parquet('", file, "')"
      )
      if (verbose)
        cat(q, "\n")
      if (method == "st_read") {
        ret = sf::st_read(conn, query = q, geometry_column = "geometry", as_tibble=T) |>
          sf::st_set_crs(crs_dest)
      } else {
        ret = tbl(conn, sql(q)) |>
          tbl2sf(crs=crs_dest)
      }
    } else if (method == "duckarrow") {
      ret = tbl_pqt(conn, file) |>
        tbl2sf(crs=crs_dest)
    }
  }
  ret
}

#' Permet d'écrire un fichier geoparquet en sf
#' @export
write_geoparquet <- function (sf, dsn, verbose=F)
{
  if (!inherits(sf, "sf")) {
    stop("Must be sf data format")
  }
  if (missing(dsn)) {
    stop("Missing output file")
  }
  # reference: https://github.com/geopandas/geo-arrow-spec
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
  arrow::write_parquet(tbl, sink = dsn, compression = "zstd")
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
