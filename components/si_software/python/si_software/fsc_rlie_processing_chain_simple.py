#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_common.yaml_parser import load_yaml, dump_yaml
from si_software.fsc_rlie_processing_chain import fsc_rlie_processing_chain


def search_for_closest_l2a(l1c, l2a_metadata):
    tile_id = l1c.split('_')[-2][1:]
    date_l1c = datetime.strptime(l1c.split('_')[-5], '%Y%m%dT%H%M%S')
    date_match = None
    file_match = None
    for l2a_file_loc in find_shell(l2a_metadata, expr='SENTINEL2*_L2A_T%s_*'%tile_id, is_dir=True):
        if '_PVD_ALL' in l2a_file_loc:
            continue
        date_l2a_loc = datetime.strptime(l2a_file_loc.split('_')[-5] + '000', '%Y%m%d-%H%M%S-%f')
        if date_l2a_loc >= date_l1c:
            continue
        if date_l2a_loc < date_l1c - timedelta(30):
            continue
        if date_match is not None:
            if abs(date_l2a_loc - date_l1c) >= (date_match - date_l1c):
                continue
        date_match, file_match = date_l2a_loc, l2a_file_loc
    return file_match



def is_l2a(l2a_path_or_name):
    try:
        _ = universal_l2a_sen2cor_id(os.path.basename(l2a_path_or_name))
    except:
        return False
    return True



def fsc_rlie_processing_chain_simple(l1c, output_dir, aux_path, maja_config_dir, l2a_metadata=None, l1c_backward=None, nprocs=1, max_cloud=90., temp_dir=None, \
    auto_switch_init_no_l2a_metadata_match=False):
    
    tile_id = l1c.split('_')[-2][1:]
    os.makedirs(output_dir, exist_ok=True)
    if temp_dir is None:
        temp_dir = os.path.join(output_dir, 'temp')
    os.makedirs(temp_dir, exist_ok=True)
    if l1c_backward is not None:
        if isinstance(l1c_backward, str):
            l1c_backward = [l1c_backward]
        assert isinstance(l1c_backward, list)
        if len(l1c_backward) == 0:
            l1c_backward = None
    
    dico = dict()
    dico['output_dir'] = output_dir
    dico['temp_dir'] = temp_dir
    dico['copy_input_files_to_temp_dir'] = False
    dico['delete_temp_dir_on_error'] = False
    dico['delete_temp_dir_on_success'] = True
    dico['dias_fsc_rlie_job_id'] = None
    dico['verbose'] = 1
    dico['nprocs'] = nprocs
    dico['max_ram'] = 4096

    dico['dem_dir'] = os.path.join(aux_path, 'eu_dem/{0}/S2__TEST_AUX_REFDE2_T{0}_0001'.format(tile_id))
    
    #maja
    dico['maja'] = dict()
    dico['maja']['static_parameter_files_dir'] = os.path.join(maja_config_dir, 'maja_static')
    dico['maja']['user_config_dir'] = os.path.join(maja_config_dir, 'maja_userconf')
    dico['maja']['mode'] = 'init'
    dico['maja']['l1c_file'] = l1c
    if l2a_metadata is not None:
        assert l1c_backward is None
        dico['maja']['mode'] = 'nominal'
        if is_l2a(l2a_metadata):
            dico['maja']['l2a_file'] = l2a_metadata
        else:
            dico['maja']['l2a_file'] = search_for_closest_l2a(l1c, l2a_metadata)
            if (dico['maja']['l2a_file'] is None) and auto_switch_init_no_l2a_metadata_match:
                dico['maja']['mode'] = 'init'
    elif l1c_backward is not None:
        dico['maja']['mode'] = 'backward'
        dico['maja']['l1c_file'] = l1c_backward + [l1c] #no check, we are very confident in our users which are very skilled ^^
    
    dico['maja']['product_mode_overide'] = 0
    dico['maja']['save_output_l2a_file'] = True
    dico['maja']['debug'] = False
    dico['maja']['max_cloud_cover_acceptable_percent'] = max_cloud
    dico['maja']['remove_sre_bands'] = True
    dico['maja']['max_processing_time'] = 14400
    
    #lis
    dico['lis'] = dict()
    dico['lis']['tree_cover_density'] = os.path.join(aux_path, 'tree_cover_density/{0}/TCD_{0}.tif'.format(tile_id))
    dico['lis']['water_mask'] = os.path.join(aux_path, 'eu_hydro/raster/20m/{0}/eu_hydro_20m_{0}.tif'.format(tile_id))
    dico['lis']['max_processing_time'] = 600
    
    #ice
    dico['ice'] = dict()
    dico['ice']['generate_product'] = True
    dico['ice']['river_shapefile'] = os.path.join(aux_path, 'eu_hydro/shapefile/{0}/eu_hydro_{0}.shp'.format(tile_id))
    dico['ice']['hrl_flags_file'] = os.path.join(aux_path, 'hrl_qc_flags/{0}/hrl_qc_flags_{0}.tif'.format(tile_id))
    dico['ice']['max_processing_time'] = 600
    if not (os.path.exists(dico['ice']['river_shapefile']) and os.path.exists(dico['ice']['hrl_flags_file'])):
        dico['ice']['generate_product'] = False
        dico['ice']['river_shapefile'] = None
        dico['ice']['hrl_flags_file'] = None
        
    dump_json(dico, filepath=os.path.join(output_dir, 'main_input_parameters.json'), indent=4)
    dump_yaml(dico, filename=os.path.join(output_dir, 'main_input_parameters.yaml'))
    
    fsc_rlie_processing_chain(dico)
    
    

if __name__ == '__main__':
    

    import argparse
    parser = argparse.ArgumentParser(description='This script is used to launch L1A, FSC and RLIE product generation on an S2 tile from level 1C Sentinel-2 product(s). Uses MAJA, LIS and ICE codes.')
    parser.add_argument("--l1c", type=str, required=True, help='L1C file from which L2A, FSC and RLIE will be computed')
    parser.add_argument("--output_dir", type=str, required=True, help='output_dir')
    parser.add_argument("--aux_path", type=str, required=True, help='aux data path')
    parser.add_argument("--maja_config_dir", type=str, required=True, help='maja_config_dir')
    ex_grp = parser.add_mutually_exclusive_group(required=False)
    ex_grp.add_argument("--l2a_metadata", type=str, help='L2A or L2A metadata file or dir containing l2as to search from to use for nominal maja mode')
    ex_grp.add_argument("--l1c_backward", type=str, action='append', help='list of L1C files to use for backward mode')
    parser.add_argument("--nprocs", type=int, default=1, help='nprocs, default is 1')
    parser.add_argument("--max_cloud", type=float, default=90., help='max_cloud, default is 90.')
    parser.add_argument("--temp_dir", type=str, help='temp_dir')
    parser.add_argument("--auto_switch_init_no_l2a_metadata_match", action='store_true', help='switch to init mode if not matching l2a metadata found. off by default')
    args = parser.parse_args()
    
    fsc_rlie_processing_chain_simple(args.l1c, args.output_dir, args.aux_path, args.maja_config_dir, l2a_metadata=args.l2a_metadata, l1c_backward=args.l1c_backward, \
        nprocs=args.nprocs, max_cloud=args.max_cloud, temp_dir=args.temp_dir, auto_switch_init_no_l2a_metadata_match=args.auto_switch_init_no_l2a_metadata_match)
    
    
    
