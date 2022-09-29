#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" 
    raster_perimeter
"""

from si_common.common_functions import *
from si_geometry.geometry_functions import *


def check_tile_id(tile_id):
    if 'T' in tile_id:
        return check_s2tile_name(elem.split('_')[tile_id_slot])
    elif tile_id[0] in ['E', 'W']:
        return check_laeatile_name(tile_id)
    raise MainArgError('tile_id format not recognised')


def make_metadata_files_from_tiles(input_tile_folder, output_file_prefix, tile_id_slot=-2):

    tile_dict = dict()
    elems = os.listdir(input_tile_folder)
    files_as_input = any(['.tif' in elem.lower() for elem in elems])
    if files_as_input:
        if tile_id_slot is None:
            raise MainArgError('unless input_tile_folder contains subfolders that are S2/LAEA tile ids, tile_id_slot parameter must be filled')
        #input_tile_folder contains .tif files (not case sensible) : all other files or folders are ignored
        for elem in elems:
            elem_path = os.path.join(input_tile_folder, elem)
            if not os.path.isfile(elem_path):
                continue
            if '.tif' not in elem.lower():
                continue
            if elem.lower().split('.')[-1] != 'tif':
                continue
            tile_name = check_tile_id(elem.split('_')[tile_id_slot].split('.')[0])
            if tile_name in tile_dict:
                raise MainArgError('tile_id %s occured twice'%tile_name)
            tile_dict[tile_name] = elem_path
    else:
        #input_tile_folder contains subfolders that contain .tif files : the first .tif file found within subfolders is designated as the reference tif for this subfolder
        #if no .tif files are present within a subfolder, an error is raised
        #all files present in input_tile_folder are ignored
        for elem in elems:
            elem_path = os.path.join(input_tile_folder, elem)
            if not os.path.isdir(elem_path):
                continue
            if '_' in elem:
                if tile_id_slot is None:
                    raise MainArgError('unless input_tile_folder contains subfolders that are S2 tile ids, tile_id_slot parameter must be filled')
                tile_name = check_s2tile_name(elem.split('_')[tile_id_slot])
            else:
                if tile_id_slot is not None:
                    raise MainArgError('if input_tile_folder contains subfolders that are S2 tile ids, tile_id_slot parameter cannot be filled')
                tile_name = check_s2tile_name(elem)
            if tile_name in tile_dict:
                raise MainArgError('S2 tile_id %s occured twice'%tile_name)
            print('searching folder %s ...'%elem)
            candidates = search_folder_structure(elem_path, regexp='*.tif', maxdepth=None, object_type='f', case_sensible=False) + \
                search_folder_structure(elem_path, regexp='*.jp2', maxdepth=None, object_type='f', case_sensible=False)
            if len(candidates) == 0:
                raise MainInputFileError('could not find a tif file in %s'%elem_path)
            tile_dict[tile_name] = candidates[0]
    
    if len(tile_dict.keys()) == 0:
        raise MainInputFileError('no .tif/.jp2 files found')
            
    make_metadata_files_from_tile_dict(tile_dict, output_file_prefix)

            
            
            
def make_metadata_files_from_tile_dict(tile_dict, output_file_prefix):
    
    output_dir = os.path.dirname(output_file_prefix + '.shp')
    if not os.path.exists(output_dir):
        os.system('mkdir -p %s'%output_dir)
    
    proj_out = 'epsg:4326'
        
    gdal_info_dict = dict()
    with fiona.open(output_file_prefix + '.shp', 'w', 'ESRI Shapefile', {'geometry': 'Polygon', 'properties': {'tile_name': 'str'}}) as ds:
        for tile_name, filepath in tile_dict.items():
            print(tile_name)
            raster_obj = RasterPerimeter(filepath)
            gdal_info_dict[tile_name] = raster_obj.info
            polygon_out = raster_obj.projected_perimeter(proj_out)
            ds.write({'geometry': mapping(polygon_out), 'properties': {'tile_name': tile_name}})
            
    with open(output_file_prefix + '_gdal_info.json', mode='w') as ds:
        json.dump(gdal_info_dict, ds)
        
    with open(output_file_prefix + '.prj', mode='w') as ds:
        ds.write(CRS.from_user_input(proj_out).to_wkt())
        
        
    
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_tile_folder", help="Path to the folder containing tiles (TIF files, or folders containing tif/jp2 files)", type=str)
    parser.add_argument("output_file_prefix", help="Path to the output shapefile", type=str)
    parser.add_argument("--tile_id_slot", help="slot between 2 underscores (_) in input_tile_folder/files or input_tile_folder/folders where tile_id can be read. " + \
        "By default, tile_id is read at slot -2 in file/folders except if the folder itself is the tile id.", type=int)
    args = parser.parse_args()
    
    make_metadata_files_from_tiles(args.input_tile_folder, args.output_file_prefix, tile_id_slot=args.tile_id_slot)
