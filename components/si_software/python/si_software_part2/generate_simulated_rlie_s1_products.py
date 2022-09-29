#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_geometry.geometry_functions import *
from si_software_part2.s1_utils import *
from si_software_part2.generate_simulated_rlie_s1_product_over_s2_tile import generate_simulated_rlie_s1_product_over_s2_tile



def generate_simulated_rlie_s1_products(output_dir, json_inputs, dem_dir, euhydro_shapefile, hrl_flags_dir, s1grd_path, s2tiles_eea39_gdal_info=None, product_geometries=None, temp_dir=None, \
    overwrite=False, verbose=1):
        
    datetime_now = datetime.utcnow()

    #get S1 info
    dico_s1 = get_s1_product_info(s1grd_path)
    
        
    #main output dir
    output_dir = os.path.abspath(output_dir)
    #check that output folder is empty if it exists, and create it if it does not exist
    if os.path.exists(output_dir):
        if overwrite:
            shutil.rmtree(output_dir)
        elif len(os.listdir(output_dir)) > 0:
            raise Exception('output directory must be empty or --overwrite option must be used')
    os.makedirs(output_dir, exist_ok=True)
    
    #temp dir
    if temp_dir is not None:
        os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='fakerlie')
    
    try:
    
        #get list of S2 tiles that need to be processed
        if product_geometries is not None:
            if isinstance(product_geometries, str):
                product_geometries = load_intersecting_tile_ids_and_geometries(product_geometries)
            else:
                assert isinstance(product_geometries, dict)
        else:
            assert product_geometries is None
            assert s2tiles_eea39_gdal_info is not None
            product_geometries = compute_intersecting_tile_ids_and_geometries(s1grd_path, s2tiles_eea39_gdal_info, temp_dir=temp_dir_session)
            
        #get json inputs from file if its not already a dict
        if not isinstance(json_inputs, dict):
            with open(json_inputs) as ds:
                json_inputs = json.load(ds)
            
        #go through all tiles and generate products
        for tile_id in product_geometries.keys():
            product_id_loc = 'RLIE_%s_%s_T%s_%s_%s'%(dico_s1['acquisition_start_date'].strftime('%Y%m%dT%H%M%S'), dico_s1['satellite'], tile_id, json_inputs['Configuration']['ProductVersion'], '1')
            generate_simulated_rlie_s1_product_over_s2_tile(output_dir, product_id_loc, os.path.join(dem_dir, 'dem_20m_%s.tif'%tile_id), euhydro_shapefile, \
                json_inputs['Configuration'], after_si_software_mode=False, temp_dir=temp_dir_session, product_geometry=product_geometries[tile_id])
            
    finally:
        shutil.rmtree(temp_dir_session)
        
    
########################################
if __name__ == '__main__':
    
    try:
        import argparse
        parser = argparse.ArgumentParser(description='This script is used to generate simulated RLIE S1 products in the same format as those from ProcessRiverIce.')
        parser.add_argument("--s1grd", type=str, required=True, help='path to S1 GRD .SAFE folder')
        parser.add_argument("--output_dir", type=str, required=True, help='path to output directory (will be created if missing, must be empty unless --overwrite option is used)')
        parser.add_argument("--json_inputs", type=str, required=True, help='JSON file containing template paths, ProductVersion, etc...')
        parser.add_argument("--euhydro_shapefile", type=str, required=True, help='path to ASTRI eu_hydro_3035.shp shapefile')
        parser.add_argument("--s2tiles_eea39_gdal_info", type=str, required=True, help='path to s2tiles_eea39_gdal_info.json')
        parser.add_argument("--hrl_flags", type=str, required=True, help='path to ASTRI HRL_FLAGS (same as for part 1)')
        parser.add_argument("--dem_path", type=str, required=True, help='path to DEM directory (either containing MAJA structure like for part 1 tile_id/.../.../dem_20m.tif, ' + \
            'or directly dem_20m_${tile_id}.tif files)')
        parser.add_argument("--precomputed_product_geometries_file", type=str, help='path to precomputed_product_geometries_file containing {tile_id: geom.wkt} dict')
        parser.add_argument("--temp_dir", type=str, help='path to temporary directory, current working directory by default')
        parser.add_argument("--overwrite", action='store_true', help='overwrite output directory if it exists')
        parser.add_argument("--verbose", type=int, help='verbose level (minimum is 1, default is minimum)')
        args = parser.parse_args()
        
    except Exception as ex:
        print(str(ex))
        sys.exit(exitcodes.wrong_input_parameters)
        
    #main function
    generate_simulated_rlie_s1_products(args.output_dir, args.json_inputs, args.dem_path, args.euhydro_shapefile, args.hrl_flags, args.s1grd, s2tiles_eea39_gdal_info=args.s2tiles_eea39_gdal_info, \
        product_geometries=args.precomputed_product_geometries_file, temp_dir=args.temp_dir, overwrite=args.overwrite, verbose=args.verbose)

