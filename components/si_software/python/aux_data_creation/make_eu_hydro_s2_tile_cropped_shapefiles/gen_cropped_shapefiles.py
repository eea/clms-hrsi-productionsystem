#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil, subprocess
import tempfile
import time
import fiona
import json

import si_utils.crop_shapefiles
from aux_data_creation.aux_data_list_files import aux_data_list_files

from si_reprocessing import install_cnes

assert sys.version_info.major >= 3



        
        
def main(eea39_gdal_info_file, src_shapefile, output_dir, pbs_mode=True):

    #get tile names
    with open(eea39_gdal_info_file) as ds:
        tile_info = json.load(ds)
        
    if pbs_mode:
        if shutil.which('qsub') is None:
            raise Exception('qsub not found, cannot work in PBS mode')
        tmp_dir_main = os.path.abspath(tempfile.mkdtemp(prefix='tmplocxxx_', dir='.'))
        crop_script_path = os.path.abspath(si_utils.crop_shapefiles.__file__)
        import load_cnes_env
        load_env_cnes_path = os.path.abspath(load_cnes_env.__file__)
    
    cwd = os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    ntiles = len(tile_info)
    for i0, (tile_id, gdal_info_loc) in enumerate(tile_info.items()):
    
        output_dir_loc = os.path.join(output_dir, tile_id)
        output_path = os.path.join(output_dir_loc, 'eu_hydro_%s.shp'%tile_id)
        if os.path.exists(output_path) or os.path.exists(os.path.join(output_dir_loc, 'nodata')):
            continue
        elif os.path.exists(output_dir_loc):
            shutil.rmtree(output_dir_loc)
        
        if not pbs_mode:
            print('Processing tile %s (%d/%d)'%(tile_id, i0+1, ntiles))
            si_utils.crop_shapefiles.crop_shapefile_to_tile(src_shapefile, output_path, gdal_info_loc, redo=False, add_nodata_file_if_empty_geometry=True, verbose=0)
        else:
            cmd = ['source %s'%install_cnes.activate_file]
            cmd += ["%s %s %s --tile_dict_json_path=%s --tile_id=%s --add_nodata_file_if_empty_geometry"%(crop_script_path, src_shapefile, output_path, eea39_gdal_info_file, tile_id)]
            with open('%s/run_%s.sh'%(tmp_dir_main, tile_id), mode='w') as ds:
                ds.write('%s\n'%('\n'.join(cmd)))
            os.system('chmod u+x %s/run_%s.sh'%(tmp_dir_main, tile_id))
            txt_pbs = ['#!/bin/bash', '#PBS -N shapefile_%s'%tile_id, '#PBS -l select=1:ncpus=1:mem=8000mb:os=rh7', '#PBS -l walltime=01:00:00']
            txt_pbs += ['%s/run_%s.sh'%(tmp_dir_main, tile_id)]
            with open('%s/run_%s.pbs'%(tmp_dir_main, tile_id), mode='w') as ds:
                ds.write('%s\n'%('\n'.join(txt_pbs)))
            os.chdir(tmp_dir_main)
            subprocess.check_call(['qsub', 'run_%s.pbs'%tile_id])
            os.chdir(cwd)

        aux_data_list_files(output_dir)
        

        
    
if __name__ == '__main__':

    
    import argparse
    parser = argparse.ArgumentParser(description="generate cropped shapefiles for each S2 tile", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--eea39_gdal_info_file", type=str, required=True, help='eea39_gdal_info_file')
    parser.add_argument("--src_shapefile", type=str, required=True, help='src_shapefile')
    parser.add_argument("--output_dir", type=str, required=True, help='output_dir')
    parser.add_argument("--pbs_mode", action="store_true", help='pbs_mode')
    args = parser.parse_args()
    
    main(args.eea39_gdal_info_file, args.src_shapefile, args.output_dir, pbs_mode=args.pbs_mode)



