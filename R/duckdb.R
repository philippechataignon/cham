#' Crée une connexion globale duckdb 'conn'
#' @export
set_conn <- function(ext = c("core", "all", "none"))
{
  ext = match.arg(ext)
  if (exists("conn", env=.GlobalEnv)) {
    if (!inherits(.GlobalEnv$conn, "duckdb_connection")) {
      stop("Existing 'conn' global object is not a duckdb connection")
    }
    if (DBI::dbIsValid(.GlobalEnv$conn)) {
      return(invisible(.GlobalEnv$conn))
    }
  }
  conn <<- DBI::dbConnect(
    duckdb::duckdb(),
    bigint = "integer64"
  )
  DBI::dbExecute(conn, "
      set threads = 4;
      set preserve_insertion_order = 'false'
    ")
  if (site == "aus") {
    DBI::dbExecute(conn, "
        set extension_directory = 'U:/extensions'
      ")
  }
  if (ext %in% c("core", "all")) {
    DBI::dbExecute(
      conn, "
      LOAD httpfs;
      LOAD spatial;
      CALL register_geoarrow_extensions();
    ")
    refresh_secret(conn)
  }
  if (ext %in% c("all")) {
    DBI::dbExecute(
      conn, "
      LOAD h3;
      LOAD read_stat;
    ")
  }
  invisible(conn)
}

#' Supprime la connexion globale duckdb 'conn'
#' @export
unset_conn <- function()
{
  if (exists("conn", env=.GlobalEnv)
      && inherits(.GlobalEnv$con, "duckdb_connection")
      && DBI::dbIsValid(.GlobalEnv$con))
  DBI::dbDisconnect(.GlobalEnv$con)
  try(rm(list="conn", envir = .GlobalEnv))
  invisible()
}

#' Installe les extensions duckdb
#' @export
install_extensions <- function(conn, ext = c("core", "all", "none"))
{
  ext = match.arg(ext)
  if (ext %in% c("core", "all")) {
    DBI::dbExecute(
      conn, "
      INSTALL httpfs;
      INSTALL spatial;
      "
    )
  }
  if (ext %in% c("all")) {
    DBI::dbExecute(
      conn, "
      INSTALL h3 from community;
      INSTALL read_stat from community;
      "
    )
  }
}

#' Renvoie une connexion duckdb
#' @param dbdir : nom du la base duckdb, par défaut base stockée en mémoire
#' @param new : si TRUE, force la création d'une nouvelle connexion, FALSE ar défaut
#' @param simple : si TRUE, renvoit une connexion comme un simple dbConnect, FALSE par défaut
#' @export
get_conn <- function(dbdir = ":memory:", new = F, simple = F) {
  # si connexion absente ou invalide ou changement de dbdir, nouvelle connexion
  if (
    (new || !exists("conn") || !DBI::dbIsValid(conn)) || basename(conn@driver@dbdir) != dbdir
  ) {
    conn = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = dbdir,
      bigint = "integer64"
    )
    DBI::dbExecute(conn, "
      set threads = 4;
      set preserve_insertion_order = 'false'
    ")
  }
  if (site == "aus") {
    DBI::dbExecute(conn, "
      set extension_directory = 'U:/extensions';
    ")
    refresh_secret(conn)
  }
  if (site %in% c("ssp", "pc")) {
    DBI::dbExecute(
      conn, "
      INSTALL httpfs;
      LOAD httpfs;
      SET s3_url_style = 'path';
      INSTALL spatial;
      LOAD spatial;
      CALL register_geoarrow_extensions();
      INSTALL h3 from community;
      LOAD h3;
    ")
  }
  if (site == "ls3") {
    DBI::dbExecute(
      conn, "
      set custom_extension_repository = 'https://nexus.insee.fr/repository/duckdb-extensions/';
      LOAD httpfs;
      SET s3_url_style = 'path';
      LOAD spatial;
      CALL register_geoarrow_extensions();
      SET temp_directory = '/tmp/duckdb_swap';
    ")
    refresh_secret(conn)
  }
  conn
}

#' Renvoit une table duckdb depuis un fichier parquet y.c S3
#' @param conn : connexion duckdb
#' @param path : chemin de la table/répertoire parquet
#' @param level : nombre de niveaux dans le cas de fichiers parquet partitionnés,
#' par défaut 0 si pas de partionnement
#' @param lower : si TRUE, les variables sont converties en minuscules
#' @return Liste de tables duckdb
#' @export

tbl_pqt <- function(conn, path, level = 0, lower = FALSE, verbose = FALSE) {
  # si les paths se terminent par un /, alors on ajoute *.parquet
  if (
    level > 0 ||
      (level == 0 && all(substr(path, nchar(path), nchar(path)) == "/"))
  ) {
    niv <- paste0(rep('/*', level + 1), collapse = "")
    path <- paste0(path, niv)
  }
  cmd <- paste0(
    "read_parquet([",
    paste(paste0("'", path, "'"), collapse = ","),
    "], hive_partitioning = true)"
  )
  if (verbose) {
    cat(cmd, "\n")
  }
  table <- dplyr::tbl(conn, cmd)
  if (lower) {
    table <- dplyr::rename_with(table, tolower)
  }
  table
}

#' Renvoit une liste de tables duckdb depuis une liste
#' de chemins vers des fichiers/répertoires parquet
#' @param conn : connexion duckdb
#' @param liste : liste de chemins
#' @export
tbl_list <- function(conn, paths, level = 0, lower = FALSE, verbose = FALSE) {
  lapply(
    paths,
    function(x) {
      tbl_pqt(conn, x, level, lower, verbose)
    }
  )
}

site = get_site()
if (site == "ls3") {
  s3perso <- paste0("s3://travail/user-", Sys.getenv("IDEP"))
} else if (site == "ssp") {
  s3perso <- paste0("s3://", Sys.getenv("IDEP"))
} else {
  s3perso <- "~/work"
}

if (site == "ls3") {
  s3expl <- "s3://insee/sern-div-exploitations-statistiques-rp"
} else if (site == "ssp") {
  s3expl <- paste0("s3://", Sys.getenv("IDEP"), "/exploitation")
} else {
  s3expl <- "~/work/insee"
}

#' Bucket perso
#' @name s3perso
#' @export
s3perso

#' Bucket exploitations statistiques
#' @name s3expl
#' @export
s3expl

#' Crée un secret à partir des variables env S3
#' @param conn : connexion duckdb
#' @return Code retour duckdb
#' @export
refresh_secret <- function(conn) {
  DBI::dbExecute(
    conn,
    paste0(
      "CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        URL_STYLE 'path',
        REGION 'us-east-1',",
      "ENDPOINT '",
      Sys.getenv("AWS_S3_ENDPOINT"),
      "',",
      "KEY_ID '",
      Sys.getenv("AWS_ACCESS_KEY_ID"),
      "',",
      "SECRET '",
      Sys.getenv("AWS_SECRET_ACCESS_KEY"),
      "',",
      "SESSION_TOKEN '",
      Sys.getenv("AWS_SESSION_TOKEN"),
      "'
      )"
    )
  )
}

#' Attach une base duckdb à une connexion
#' @param conn: connexion duckdb, peut être obtenu par la fonction get_conn
#' @param path: chemin de la base duckdb, ".duckdb" est ajouté automatiquement et la base est dans le répertoire
#'        (s3perso)/duckdb
#' @param db: nom de l'alias de la base
#' @param crypt: si TRUE, passe la valeur de DUCKDB_ENCRYPTION_KEY comme clé de la base cryptée
#' @return Nom de la base duckdb
#' @export
attach_db <- function(conn, path, db, crypt=FALSE) {
  DBI::dbExecute(conn, paste("DETACH DATABASE IF EXISTS", db))
  cmd = paste0("ATTACH '", path, "' as ", db)
  if (crypt) {
    cmd = paste0(
      cmd,
      "(encryption_key '",
      Sys.getenv("DUCKDB_ENCRYPTION_KEY"),
      "')"
    )
  }
  DBI::dbExecute(conn, cmd)
  invisible(db)
}

#' Renvoie une liste de tables depuis une database duckdb
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @param db: nom de la database
#' @return Liste de tables dplyr
#' @export
tbl_db <- function(conn, db, verbose=FALSE) {
  tables = dbGetQuery(conn, paste("SHOW TABLES FROM", db))$name
  if (verbose) {
    print(tables)
  }
  lapplyn(tables, function(x) dplyr::tbl(conn, paste0(db, ".", x)))
}

#' Renvoie une liste de tables depuis une base duckdb
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @param name: nom de la base duckdb, ".duckdb" est ajouté automatiquement et la base est dans le répertoire
#'        (s3perso)/duckdb
#' @param db: nom de l'alias de la base, par défaut path
#' @param crypt: si TRUE, passe la valeur de DUCKDB_ENCRYPTION_KEY comme clé de la base cryptée
#' @return Liste de tables
#' @export
tbl_duckdb <- function(conn, name, db=NULL, crypt=FALSE) {
  if (is.null(db)) {
    db = name
  }
  if (grepl("crypt", name, fixed=T)) {
    crypt = TRUE
  }
  db = attach_db(conn, path = file.path(s3perso, "duckdb", paste0(name, ".duckdb")), db, crypt=crypt)
  tbl_db(conn, db)
}
