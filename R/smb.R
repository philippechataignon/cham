#' Commandes vers partages Windows
#' @param cmd Commande dont '(m)get' '(m)put' et 'ls'. Voir exemples.
#' @param lettre Lettre du lecteur distant
#' @param dir Répertoire distant
#' @param file Chemin distant, répertoire à créer ou fichier selon la commande
#' @param lpath Chemin local, répertoire ou fichier selon la commande
#' @param mask Limite les commandes mget/mput, par défaut '*'
#' @examples
#' smb("ls", "X")
#' smb("ls", "X", "HAB-MaD-SeRN/naiss")
#' smb("get", "X", "HAB-MaD-SeRN/naiss", "naiss_2017.csv", "~/work/data")
#' smb("mget","X", "HAB-MaD-SeRN/naiss", lpath="~/work/data")
#' smb("mget","X", "HAB-MaD-SeRN/naiss", lpath="~/work/data", mask = "naiss_202*.csv")
#' smb("deltree", "X", "HAB-MaD-SeRN", "naiss_copie") # DANGER !
#' smb("mkdir", "X", "HAB-MaD-SeRN", "naiss_copie")
#' smb("rmdir", "X", "HAB-MaD-SeRN", "naiss_copie")
#' smb("mkdir", "X", "HAB-MaD-SeRN", "naiss_copie")
#' smb("put", "X", "HAB-MaD-SeRN/naiss_copie", "naiss_2022.csv", "~/work/data/naiss_2022.csv")
#' smb("mput", "X", "HAB-MaD-SeRN/naiss_copie", lpath = "~/work/data", mask="*.rds")
#' @export
smb <- function(cmd, lettre, dir=".", file=NULL, lpath = ".", mask = "*")
{
  cmds =list(
    'get' = 'lcd "{lpath}"; cd "{dir}"; get "{file}"',
    'mget' = 'lcd "{lpath}"; cd "{dir}"; mask \'*\'; recurse ON; prompt OFF; mget "{mask}"',
    'put' = 'cd "{dir}"; put "{lpath}" "{file}"',
    'mput' = 'lcd "{lpath}"; cd "{dir}"; mask \'*\'; recurse ON; prompt OFF; mput "{mask}"',
    'rm' = 'cd "{dir}"; rm "{file}"',
    'deltree' = 'cd "{dir}"; deltree "{file}"',
    'rmdir' = 'cd "{dir}"; rmdir "{file}"',
    'mkdir' = 'cd "{dir}"; mkdir "{file}"',
    "ls" = 'cd "{dir}"; ls'
  )
  cmd = match.arg(cmd, names(cmds))
  # Assure existence répertoire local
  if (cmd == "mget") {
    dir.create(lpath, recursive = T, showWarnings = F)
  }
  if (is.null(file) && cmd %in% c("get", "put", "rm", "deltree", "rmdir", "mkdir")) {
    stop("'file=' doit être renseigné pour cette commande")
  }
  cmd = cmds[[cmd]]
  uncs = list(
    "P" = "//pd_aus_POL/POLES",
    "U" = "//pd_aus_ESP_U/ESPERT_U",
    "V" = "//pd_aus_ECH/ECHANGES",
    "W" = "//pd_aus_GEN/GEN",
    "X" = "//pd_aus_HAB/COFFRES",
    "Z" = "//pd_aus_ESP_Z/ESPERT_Z"
  )
  lettre = toupper(lettre)
  lettre = match.arg(lettre, names(uncs))
  unc = uncs[[lettre]]
  lpath = path.expand(lpath)
  idep = Sys.getenv("IDEP")
  cat("Cmd:", glue::glue(cmd), "\n")
  smbcmd = glue::glue("smbclient -U AD/{idep} ",
    "--password {Sys.getenv('PASSWORDINSEE')} {unc} ",
    "-c '{glue::glue(cmd)}'"
  )
  system(smbcmd)
}
