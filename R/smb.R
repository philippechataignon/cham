#' Commandes vers partages Windows
#' @param cmd Commande parmi "get" "mget" "put" "mput" "mkdir" "ls" "lsd"
#' @param path Chemin de fichier
#' @param dir Répertoire
#' @export
smb <- function(cmd, path, dir = ".")
{
  cmds =list(
    "get" = "lcd {dir}; cd {rdir}; get {file}",
    "mget" = "lcd {dir}; cd {rdir}; mask ''; recurse ON; prompt OFF; mget *",
    "put" = "lcd {dir}; cd {rdir}; put {file}",
    "mput" = "lcd {dir}; cd {rdir}; mask ''; recurse ON; prompt OFF; mput *",
    "mkdir" = "cd {rdir}; mkdir {file}",
    "ls" = "ls",
    "lsd" = "cd {rdir}; ls"
  )
  cmd = match.arg(cmd, names(cmds))
  cmd = cmds[[cmd]]
  uncs = list(
    "P" = "//pd_aus_POL/POLES",
    "U" = "//pd_aus_ESP_U/ESPERT_U",
    "V" = "//pd_aus_ECH/ECHANGES",
    "W" = "//pd_aus_GEN/GEN",
    "X" = "//pd_aus_HAB/COFFRES",
    "Z" = "//pd_aus_ESP_Z/ESPERT_Z"
  )
  dir = path.expand(dir)
  idep = Sys.getenv("IDEP")
  ret = stringi::stri_match(path, regex="^(.+?)/(.+)/([^/]*)$")
  if (any(is.na(ret)))
    stop("'path' n'est pas de la forme lettre/dir1/(dir2)/(file)")
  lettre = match.arg(ret[2], names(uncs))
  unc = uncs[[lettre]]
  rdir = ret[3]
  file = ret[4]
  cat("Cmd:", glue::glue(cmd), "\n")
  smbcmd = glue::glue("smbclient -U AD/{idep} ",
    "--password {Sys.getenv('PASSWORDINSEE')} {unc} ",
    "-c '{glue::glue(cmd)}'"
  )
  system(smbcmd)
}
