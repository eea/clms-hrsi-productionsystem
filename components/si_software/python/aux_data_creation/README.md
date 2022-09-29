Steps to generate aux data on CNES Cluster (`hidden_value`):
+ create merged eu hydro water mask using `generate_merged_eu_hydro_water_mask`
+ create cropped shapefiles using `make_eu_hydro_s2_tile_cropped_shapefiles`.
  + Those shapefiles are used primarily as an ICE software input for RLIE product generation.
  + WARNING : For some reason, ESA SNAP used by the ICE software fails to read the shapefiles generated bu the above script. To correct this, replace all prj files with the content of `make_eu_hydro_s2_tile_cropped_shapefiles/eu_hydro.prj`. For example, you should replace the `32TLR/eu_hydro_32TLR.shp` file contents with those from the eu_hydro.shp file.
+ create 


For all those steps, use the `./run --pbs_mode` command.

Important : At the end :
+ go to each of the folders containing the aux products (folders containing $tile_id directories), and type `find . -type f > list_files.txt`
+ go to `cosims/tests/infra/buckets` and execute `./get_file_list_bundle.sh $path_to_tf-si-aux`. This will copy all the list_file.txt to the git repo, so that aux files existence can be checked by the system.
+ then upload the `hidden_value` files to the project and op buckets on the DIAS.
