#!/bin/sh
PKG=$(Rscript -e "sink('/dev/null');devtools::document(roclets = c('rd', 'collate', 'namespace')); file=devtools::build(quiet=T); sink(); cat(file, '\n')")
echo $PKG
# echo $(basename $PKG)
curl --upload-file ${PKG} "https://nexus.insee.fr/repository/r-local/src/contrib/$(basename ${PKG})"
