#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil, subprocess
import tempfile
try:
    from osgeo import gdal
except:
    import gdal

from simple_rasterization import create_binary_mask_on_tile_from_shapefiles
from si_reprocessing import install_cnes


def simple_directory_search(fol, str_search, single_output=False, error_no_output=False):
    subdir = [os.path.join(fol, el) for el in os.listdir(fol) if str_search in el]
    if len(subdir) == 0:
        if error_no_output:
            raise Exception('no matching case found')
        return None
    if single_output:
        if len(subdir) > 1:
            print(subdir)
            raise Exception('multiple matching cases found')
        subdir = subdir[0]
    return subdir


def get_maja_dem_specific_tif_path(dem_folder, suffix):
    subdir = simple_directory_search(dem_folder, '.DBL.DIR', single_output=True, error_no_output=True)
    tif_path = simple_directory_search(subdir, suffix, single_output=True, error_no_output=True)
    return tif_path
    
    
            
def make_eu_hydro_water_mask_rasters(maja_dems_folder, hydro_shapes_folder, output_folder, output_prefix='eu_hydro_mask', resolution='R2', reprocess=False):
    
    if output_prefix is None:
        output_prefix = 'eu_hydro_mask'
    if resolution is None:
        resolution = 'R2'
    
    os.makedirs(output_folder, exist_ok=True)
    suffix = 'ALT_%s.TIF'%resolution
    dem_tif_inputs = dict()
    for el in os.listdir(maja_dems_folder):
        fol_path = os.path.join(maja_dems_folder, el)
        if (not os.path.isdir(fol_path)) or len(el) != 5:
            continue
        subfol = os.listdir(fol_path)
        assert len(subfol) == 1, 'found %d!=1 subfolder'%len(subfol)
        assert 'S2__TEST_AUX_REFDE2' in subfol[0], 'subfolder mismatch'
        dem_tif_inputs[el] = get_maja_dem_specific_tif_path(os.path.join(maja_dems_folder, el, subfol[0]), suffix)
    assert len(list(dem_tif_inputs.keys())) == 1054
        
    shapefiles = dict()
    for el in os.listdir(hydro_shapes_folder):
        fol_path = os.path.join(hydro_shapes_folder, el)
        if (not os.path.isdir(fol_path)) or len(el) != 5:
            continue
        shapefile_path = os.path.join(hydro_shapes_folder, el, 'eu_hydro_%s.shp'%el)
        if os.path.exists(os.path.join(hydro_shapes_folder, el, 'nodata')):
            assert not os.path.exists(shapefile_path)
            continue
        assert os.path.exists(shapefile_path)
        shapefiles[el] = shapefile_path

        
    for tile_name in sorted(dem_tif_inputs.keys()):
        output_fol_loc = os.path.join(output_folder, tile_name)
        output_file = os.path.join(output_fol_loc, '%s_%s.tif'%(output_prefix, tile_name))
        if os.path.exists(output_file):
            if reprocess:
                shutil.rmtree(output_fol_loc)
            else:
                continue
        elif os.path.exists(output_fol_loc):
            shutil.rmtree(output_fol_loc)

        if tile_name in shapefiles:
            shapes = [shapefiles[tile_name]]
        else:
            shapes = []
        print('Processing tile %s ...'%tile_name)
        os.makedirs(output_fol_loc)
        try:
            create_binary_mask_on_tile_from_shapefiles(dem_tif_inputs[tile_name], shapes, output_file)
        except:
            shutil.rmtree(output_fol_loc)
            raise
    
    

if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="Create binary water mask rasters using S2-tiled MAJA DEMs as raster models and S2-tiled EU-Hydro shapefiles")
    parser.add_argument("maja_dems_folder", type=str, help="S2-tiled MAJA DEMs folder")
    parser.add_argument("hydro_shapes_folder", type=str, help="S2-tiled Hydro shapefiles folder : shapefiles must be named ending with _${tile_number}.shp. Ex: euhydrorivers_32TLR.shp")
    parser.add_argument("output_folder", type=str, help="Output folder")
    parser.add_argument("--output_prefix", type=str, default='eu_hydro_mask', help="Output raster files prefix : files will be named ${output_prefix}_${tile_name}.tif")
    parser.add_argument("--resolution", choices=['R1', 'R2'], default='R2', \
        help="Output resolution. R2 will use MAJ DEM ALT_R2.TIF file as raster model and therefore produce a 20m*20m raster. " + \
        "Similarly, R1 (default), will produce a 10m*10m raster")
    parser.add_argument("--reprocess", action='store_true', help="Use this option to overwrite files if they exist. If this option is not activated, existing output files will not be reprocessed.")
    parser.add_argument("--pbs_mode", action="store_true", help='pbs_mode')
    args = parser.parse_args()
    
    if args.pbs_mode:
        
        cwd = os.getcwd()
        if shutil.which('qsub') is None:
            raise Exception('qsub not found, cannot work in PBS mode')
        tmp_dir_main = os.path.abspath(tempfile.mkdtemp(prefix='tmplocxxx_', dir='.'))
        self_script_path = os.path.abspath(__file__)
        
        cmd = ['source %s'%install_cnes.activate_file]
        cmd_loc = [self_script_path, args.maja_dems_folder, args.hydro_shapes_folder, args.output_folder, \
            '--output_prefix=%s'%args.output_prefix, '--resolution=%s'%args.resolution]
        if args.reprocess:
            cmd_loc += ['--reprocess']
        cmd += [' '.join(cmd_loc)]
        with open('%s/run.sh'%tmp_dir_main, mode='w') as ds:
            ds.write('%s\n'%('\n'.join(cmd)))
        os.system('chmod u+x %s/run.sh'%tmp_dir_main)
        txt_pbs = ['#!/bin/bash', '#PBS -N euhydroraster', '#PBS -l select=1:ncpus=4:mem=20000mb:os=rh7', '#PBS -l walltime=10:00:00']
        txt_pbs += ['%s/run.sh'%tmp_dir_main]
        with open('%s/run.pbs'%tmp_dir_main, mode='w') as ds:
            ds.write('%s\n'%('\n'.join(txt_pbs)))
        os.chdir(tmp_dir_main)
        subprocess.check_call(['qsub', 'run.pbs'])
        os.chdir(cwd)
    
    else:

        make_eu_hydro_water_mask_rasters(args.maja_dems_folder, args.hydro_shapes_folder, args.output_folder, output_prefix=args.output_prefix, resolution=args.resolution, reprocess=args.reprocess)
    
