#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" 
    raster_perimeter
"""

import os
from si_geometry.geometry_functions import RasterPerimeter
import json

def remove_shapefiles(shapefile_out):
    os.system('rm -f %s*'%shapefile_out.replace('.shp', ''))
    
    
def crop_shapefile_to_tile(source_shapefile, shapefile_out, tile_info_or_path, redo=False, add_nodata_file_if_empty_geometry=False, verbose=0):
    
    assert shapefile_out.split('.')[-1] == 'shp', 'shapefile output path must end by .shp'
    
    output_folder_existed = os.path.exists(os.path.dirname(shapefile_out))
    os.makedirs(os.path.dirname(shapefile_out), exist_ok=True)
    
    if os.path.exists(shapefile_out):
        if redo:
            remove_shapefiles(shapefile_out)
        else:
            return
        
    try:
        s2_rasterperim = RasterPerimeter(tile_info_or_path)
        valid = s2_rasterperim.clip_multipolygon_shapefile_to_raster_perimeter(source_shapefile, shapefile_out, verbose=verbose)
        if not valid and add_nodata_file_if_empty_geometry:
            with open(os.path.join(os.path.dirname(shapefile_out), 'nodata'), mode='w') as ds:
                ds.write('\n')
    except:
        if output_folder_existed:
            shutil.rmtree(os.path.dirname(shapefile_out))
        else:
            remove_shapefiles(shapefile_out)
        raise
    

        
    

if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="crop shapefiles to a raster's perimeter", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("source_shapefile", type=str, help="source shapefile path.")
    parser.add_argument("shapefile_out", type=str, help="output shapefile")
    group_ex = parser.add_mutually_exclusive_group(required=True)
    group_ex.add_argument("--model_raster_path", type=str, help="model_raster : the output shapefile with be cropped to this raster's footprint.")
    group_ex.add_argument("--tile_dict_json_path", type=str, help="path to a json file containing {tile_id: gdal_info}. --tile_id required")
    parser.add_argument("--tile_id", type=str, help="tile_id, required with the --tile_dict_json_path option")
    parser.add_argument("--redo", action="store_true", help='overwrite')
    parser.add_argument("--add_nodata_file_if_empty_geometry", action="store_true", help='add_nodata_file_if_empty_geometry')
    args = parser.parse_args()


    if args.tile_dict_json_path is not None:
        assert args.tile_id is not None, 'tile_id, required with the --tile_dict_json_path option'
        with open(args.tile_dict_json_path) as ds:
            tile_info_or_path = json.load(ds)[args.tile_id]
    else:
        tile_info_or_path = args.model_raster_path
    crop_shapefile_to_tile(args.source_shapefile, args.shapefile_out, tile_info_or_path, redo=args.redo, add_nodata_file_if_empty_geometry=args.add_nodata_file_if_empty_geometry)
    

    
