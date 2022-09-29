#!/bin/bash

set -e

fol_src=$1
fol='sip-aux'
rm -Rf $fol
mkdir $fol
cd $fol

subfolders=( "eu_dem" "eu_hydro/raster/20m" "eu_hydro/shapefile" "tree_cover_density" "hrl_qc_flags" "AOI_EEA39/s2tiles_eea39" "AOI_EEA39/laea_tiles_eea39" )
for subfolder in "${subfolders[@]}"
do
    mkdir -p ${subfolder}
    cp ${fol_src}/${subfolder}/list_files.txt ${subfolder}
done


