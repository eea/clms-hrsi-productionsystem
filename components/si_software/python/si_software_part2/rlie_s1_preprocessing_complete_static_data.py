#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_utils.rclone import Rclone
from si_software_part2.s1_utils import *

def rlie_s1_preprocessing_complete_static_data(tf_sip_aux_path, dem_dir, precomputed_product_geometries_file=None, tile_ids=None, rclone_config=None):
    
    #read tile list needed
    if tile_ids is None:
        assert precomputed_product_geometries_file is not None
        with open(precomputed_product_geometries_file) as ds:
            tile_ids = list(json.load(ds).keys())
    else:
        assert precomputed_product_geometries_file is None
        
    rclone_obj = Rclone(config_file=rclone_config)
        
    #get DEM files
    dem_files_dict, missing_dem_tiles, has_part1_structure_dem_files = get_dem_20m_paths(dem_dir, tile_ids)
    for tile_id in missing_dem_tiles:
        input_file = '{0}/eu_dem/{1}/S2__TEST_AUX_REFDE2_T{1}_0001/S2__TEST_AUX_REFDE2_T{1}_0001.DBL.DIR/dem_20m.tif'.format(tf_sip_aux_path, tile_id)
        output_dir_temp = os.path.join(dem_dir, tile_id)
        output_file_temp = os.path.join(output_dir_temp, 'dem_20m.tif')
        output_file = os.path.join(dem_dir, 'dem_20m_%s.tif'%tile_id)
        print('%s -> %s'%(input_file, output_file))
        try:
            rclone_obj.copy(input_file, output_dir_temp)
            shutil.move(output_file_temp, output_file)
        finally:
            shutil.rmtree(output_dir_temp)
    
    
    
########################################
if __name__ == '__main__':
    
    try:
        import argparse
        parser = argparse.ArgumentParser(description='This script is used to complete HRL and DEM static data for later ICE processing step using an input JSON file.')
        gr_ex = parser.add_mutually_exclusive_group(required=True)
        gr_ex.add_argument("--precomputed_product_geometries_file", type=str, help='path to precomputed_product_geometries_file containing {tile_id: geom.wkt} dict')
        gr_ex.add_argument("--tile_ids", type=str, help='single tile ids or list of tile1,tile2,...')
        parser.add_argument("--tf_sip_aux", type=str, required=True, help='path to tf_sip_aux')
        parser.add_argument("--dem_path", type=str, required=True, \
            help='path to DEM directory (either containing MAJA structure like for part 1 tile_id/.../.../dem_20m.tif, or directly dem_20m_${tile_id}.tif files)')
        parser.add_argument("--rclone_config", type=str, required=True, help='rclone config file')
        args = parser.parse_args()
        
    except Exception as ex:
        print(str(ex))
        sys.exit(exitcodes.wrong_input_parameters)
        
    if args.tile_ids is not None:
        args.tile_ids = args.tile_ids.replace(' ','').split(',')
            
        
    #main function
    rlie_s1_preprocessing_complete_static_data(args.tf_sip_aux, args.dem_path, precomputed_product_geometries_file=args.precomputed_product_geometries_file, \
        tile_ids=args.tile_ids, rclone_config=args.rclone_config)

