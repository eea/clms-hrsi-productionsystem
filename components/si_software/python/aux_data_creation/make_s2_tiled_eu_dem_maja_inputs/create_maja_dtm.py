#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil, subprocess
import tempfile
import fiona
import time

assert sys.version_info.major >= 3

from si_geometry.geometry_functions import set_gdal_otb_itk_env_vars
from aux_data_creation.aux_data_list_files import aux_data_list_files
from si_reprocessing import install_cnes



def search_folder_structure(folder_in, regexp=None, maxdepth=None, object_type=None, case_sensible=False):
    cmd = 'find %s'%folder_in
    if maxdepth is not None:
        cmd += ' -maxdepth %d'%maxdepth
    if object_type is not None:
        if object_type not in ['d', 'f']:
            raise Exception('object type %s unknown'%object_type)
        cmd += ' -type ' + object_type
    if regexp is not None:
        if case_sensible:
            cmd += ' -name ' + regexp
        else:
            cmd += ' -iname ' + regexp
    try:
        list_out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')[0:-1]
        return list_out
    except:
        return []
    

def get_valid_l1c(l1c_directory, tile_name):
    main_tile_folders = os.listdir(l1c_directory)
    if tile_name in main_tile_folders:
        candidates = search_folder_structure('%s/%s'%(l1c_directory, tile_name), regexp='*.SAFE', maxdepth=4, object_type='d')
    elif 'T' + tile_name in main_tile_folders:
        candidates = search_folder_structure('%s/T%s'%(l1c_directory, tile_name), regexp='*.SAFE', maxdepth=4, object_type='d')
    else:
        return None
    for candidate in candidates:
        subdir = '%s/GRANULE'%candidate
        try:
            subdir = subdir + '/' + os.listdir(subdir)[0]
            if os.path.exists('%s/MTD_TL.xml'%subdir) and os.path.exists('%s/IMG_DATA'%subdir):
                if any(['B01.jp2' in el for el in os.listdir('%s/IMG_DATA'%subdir)]):
                    return candidate
        except:
            continue
    return None
    
    
def check_dem(tile_dir):
    tag = os.listdir(tile_dir)
    assert len(tag) > 0, 'no dem created in directory %s'%out_dir
    assert len(tag) == 1, 'mutliple files in output directory %s where there should only be one'%out_dir
    tag = tag[0]
    assert os.path.exists(os.path.join(tile_dir, tag, tag + '.HDR'))
    for suffix in ['_SLP_R2.TIF', '_SLP_R1.TIF', '_SLC.TIF', '_ASP_R1.TIF', '_ASP_R2.TIF', '_ALT_R2.TIF', '_ALC.TIF', '_ASC.TIF', '_ALT_R1.TIF', '_MSK.TIF']:
        assert os.path.exists(os.path.join(tile_dir, tag, tag + '.DBL.DIR', tag + suffix))
    return os.path.join(tile_dir, tag)
    

def create_maja_dtm_single_tile_worker(output_dir, l1c_samples_eea39_dir, eu_dem_src_dir, src_cropped_shapefile_dir, tile_id, zone_selection_shapefile=None, redo=False, tmp_dir=None, use_25m=False):
    
    set_gdal_otb_itk_env_vars(nprocs=1)
    
    from aux_data_creation.make_s2_tiled_eu_dem_maja_inputs.DTMCreation import DTMCreator
    
    if os.path.exists(output_dir):
        if redo:
            shutil.rmtree(output_dir)
        else:
            return

    l1c_path = get_valid_l1c(l1c_samples_eea39_dir, tile_id)
    eu_hydro_shapefile_path = os.path.join(src_cropped_shapefile_dir, tile_id, 'eu_hydro_%s.shp'%tile_id)
    print('shapefile path: %s'%eu_hydro_shapefile_path)
    if not os.path.exists(eu_hydro_shapefile_path):
        assert os.path.exists(os.path.join(src_cropped_shapefile_dir, tile_id, 'nodata'))
        eu_hydro_shapefile_path = None
    if tmp_dir is None:
        if 'TMPDIR' in os.environ:
            tmp_dir = os.environ['TMPDIR']
        else:
            tmp_dir = '.'
    tmp_dir_main = tempfile.mkdtemp(dir=tmp_dir)
    
    try:
        if use_25m:
            output_status = 'no_intersection_with_src_dem'
        else:
            print('%s: using 10m DEM'%tile_id)
            creator = DTMCreator(l1c_path, None, None, os.path.join(eu_dem_src_dir, '10m'), eu_hydro_shapefile_path, waterOnly=False, mntType="EUDEM", \
                coarseRes=240, zone_selection_shapefile=zone_selection_shapefile)
            output_status = creator.run(os.path.join(tmp_dir_main, 'out'), os.path.join(tmp_dir_main, 'tmp'))
        if output_status == 'no_intersection_with_src_dem':
            print('%s: using 25m DEM'%tile_id)
            if os.path.exists(tmp_dir_main):
                shutil.rmtree(tmp_dir_main)
            tmp_dir_main = tempfile.mkdtemp(dir=tmp_dir)
            creator = DTMCreator(l1c_path, None, None, os.path.join(eu_dem_src_dir, '25m'), eu_hydro_shapefile_path, waterOnly=False, mntType="EUDEM", \
                coarseRes=240, zone_selection_shapefile=zone_selection_shapefile)
            output_status = creator.run(os.path.join(tmp_dir_main, 'out'), os.path.join(tmp_dir_main, 'tmp'))
            if output_status != 'valid':
                raise Exception('DTMCreator returned with status=%s using 25m DEM'%output_status)
        elif output_status != 'valid':
            raise Exception('DTMCreator returned with status=%s using 10m DEM'%output_status)
            
        dem_dir = check_dem(os.path.join(tmp_dir_main, 'out'))
        os.makedirs(output_dir)
        shutil.move(dem_dir, os.path.join(output_dir, os.path.basename(dem_dir)))
        shutil.rmtree(tmp_dir_main)
    except:
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        raise




        
        
        
def create_maja_dtm(output_dir, l1c_samples_eea39_dir, eu_dem_src_dir, src_cropped_shapefile_dir, zone_selection_shapefile=None, pbs_mode=True, redo=False, tmp_dir=None, use_25m=False):
    
    cwd = os.getcwd()

    #get tile names
    tile_ids = set([el for el in os.listdir(l1c_samples_eea39_dir) if '.' not in el and len(el)==5])
    ntiles = len(tile_ids)
    assert ntiles == 1054, 'found ntiles=%s!=1054'%ntiles
        
    if pbs_mode:
        if shutil.which('qsub') is None:
            raise Exception('qsub not found, cannot work in PBS mode')
        if tmp_dir is None:
            if 'TMPDIR' in os.environ:
                tmp_dir = os.environ['TMPDIR']
            else:
                tmp_dir = '.'
        tmp_dir_main = os.path.abspath(tempfile.mkdtemp(prefix='tmplocxxx_', dir=tmp_dir))
        self_script_path = os.path.abspath(__file__)
    
    if redo and os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    for i0, tile_id in enumerate(sorted(list(tile_ids))):
    
        output_dir_loc = os.path.join(output_dir, tile_id)
        if os.path.exists(output_dir_loc):
            try:
                check_dem(output_dir_loc)
                continue
            except:
                shutil.rmtree(output_dir_loc)

        if not pbs_mode:
            print('Processing tile %s (%d/%d)'%(tile_id, i0+1, ntiles))
            create_maja_dtm_single_tile_worker(output_dir_loc, l1c_samples_eea39_dir, eu_dem_src_dir, src_cropped_shapefile_dir, tile_id, \
                zone_selection_shapefile=zone_selection_shapefile, tmp_dir=tmp_dir, use_25m=use_25m)
        else:
            cmd = ['source %s'%install_cnes.activate_file]
            cmd_loc = "%s --output_dir=%s --l1c_samples_eea39_dir=%s --eu_dem_src_dir=%s --src_cropped_shapefile_dir=%s --tile_id=%s"%(self_script_path, output_dir_loc, \
                l1c_samples_eea39_dir, eu_dem_src_dir, src_cropped_shapefile_dir, tile_id)
            if zone_selection_shapefile is not None:
                cmd_loc += ' --zone_selection_shapefile=%s'%zone_selection_shapefile
            cmd += [cmd_loc]
            with open('%s/run_%s.sh'%(tmp_dir_main, tile_id), mode='w') as ds:
                ds.write('%s\n'%('\n'.join(cmd)))
            os.system('chmod u+x %s/run_%s.sh'%(tmp_dir_main, tile_id))
            txt_pbs = ['#!/bin/bash', '#PBS -N eudem_%s'%tile_id, '#PBS -l select=1:ncpus=4:mem=20000mb:os=rh7', '#PBS -l walltime=10:00:00']
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
    parser.add_argument("--output_dir", type=str, required=True, help='output_dir')
    parser.add_argument("--l1c_samples_eea39_dir", type=str, required=True, help='l1c_samples_eea39_dir')
    parser.add_argument("--eu_dem_src_dir", type=str, required=True, help='eu_dem_src_dir')
    parser.add_argument("--src_cropped_shapefile_dir", type=str, required=True, help='src_cropped_shapefile_dir')
    parser.add_argument("--zone_selection_shapefile", type=str, help='zone_selection_shapefile')
    parser.add_argument("--tile_id", type=str, help='tile_id')
    parser.add_argument("--tmp_dir", type=str, help='tmp_dir')
    parser.add_argument("--pbs_mode", action="store_true", help='pbs_mode')
    parser.add_argument("--redo", action="store_true", help='redo')
    parser.add_argument("--use_25m", action="store_true", help='Use 25m DEM. By default the program tries to use the 10m DEM and if the tile does not intersect with the 10m DEM, it switches to 25m DEM.')
    args = parser.parse_args()
    
    if args.tile_id is not None:
        create_maja_dtm_single_tile_worker(args.output_dir, args.l1c_samples_eea39_dir, args.eu_dem_src_dir, args.src_cropped_shapefile_dir, \
            args.tile_id, zone_selection_shapefile=args.zone_selection_shapefile, redo=args.redo, tmp_dir=args.tmp_dir, use_25m=args.use_25m)
    else:
        create_maja_dtm(args.output_dir, args.l1c_samples_eea39_dir, args.eu_dem_src_dir, args.src_cropped_shapefile_dir, zone_selection_shapefile=args.zone_selection_shapefile, \
            pbs_mode=args.pbs_mode, redo=args.redo, tmp_dir=args.tmp_dir, use_25m=args.use_25m)


