#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess
from correct_fsc_product_qc_layer import correct_fsc_qc_layers, change_product_version

fsc_dir = '/datalake/S2-L2B-COSIMS/data/Snow/FSC'
dem_dir = '/work/ALT/swot/aval/neige/cosims-bucketmirror/hidden_value/eu_dem'
tcd_dir = '/work/ALT/swot/aval/neige/cosims-bucketmirror/hidden_value/tree_cover_density'

def process_tile(tile_id, correct_layers=True, new_version_tag=None, temp_dir=None):
    dem_dir_path = os.path.join(dem_dir, tile_id, 'S2__TEST_AUX_REFDE2_T%s_0001'%tile_id)
    tcd_path = os.path.join(tcd_dir, tile_id, 'TCD_%s.tif'%tile_id)
    for year in os.listdir(os.path.join(fsc_dir, tile_id)):
        for month in os.listdir(os.path.join(fsc_dir, tile_id, year)):
            for day in os.listdir(os.path.join(fsc_dir, tile_id, year, month)):
                for product_id in os.listdir(os.path.join(fsc_dir, tile_id, year, month, day)):
                    product_path = os.path.join(fsc_dir, tile_id, year, month, day, product_id)
                    print(product_path)
                    try:
                        if correct_layers:
                            correct_fsc_qc_layers(product_path, dem_dir_path, tcd_path, output_path=None, temp_dir=temp_dir)
                        if version_tag is not None:
                            product_path = change_product_version(product_path, version_tag=new_version_tag)
                        os.system('chmod -R a+rx %s'%product_path)
                        print('  -> %s succeeded'%product_path)
                    except:
                        print('  -> %s failed'%product_path)


def launch_tile(tile_id, correct_layers=True, new_version_tag=None, temp_dir=None):
    
    cwd = os.getcwd()
    
    if temp_dir is None:
        if 'TMPDIR' in os.environ:
            temp_dir = os.environ['TMPDIR']
        else:
            temp_dir = '.'
    temp_dir_session = os.path.join(os.path.abspath(temp_dir), tile_id)
    os.makedirs(temp_dir_session, exist_ok=True)
       
    #build shell script to launch command that will run on node
    run_cmd = '%s --process_tile %s'%(os.path.abspath(__file__), tile_id)
    if correct_layers:
        run_cmd += ' --correct_layers'
    if new_version_tag is not None:
        run_cmd += ' --new_version_tag %s'%new_version_tag
    run_lines = ['#!/bin/bash', 'module load conda', 'conda activate gdalnew', run_cmd]
    with open(os.path.join(temp_dir_session, 'run.sh'), mode='w') as ds:
        ds.write('%s\n'%('\n'.join(run_lines)))
        
    #build PBS command file
    qsub_lines = ['#!/bin/bash', '#PBS -N %s'%tile_id, '#PBS -l select=1:ncpus=1:mem=5000mb:os=rh7', '#PBS -l walltime=72:00:00']
    qsub_lines += ['', 'bash %s/run.sh'%temp_dir_session]
    with open(os.path.join(temp_dir_session, 'run.pbs'), mode='w') as ds:
        ds.write('%s\n'%('\n'.join(qsub_lines)))
        
    #launch PBS command
    os.chdir(temp_dir_session)
    subprocess.check_call(['qsub', 'run.pbs'])
    os.chdir(cwd)


def launch_all(correct_layers=args.correct_layers, new_version_tag=args.new_version_tag, temp_dir=None):
    if temp_dir is not None:
        temp_dir = os.path.abspath(temp_dir)
    for tile_id in os.listdir(fsc_dir):
        if os.path.isdir(os.path.join(fsc_dir, tile_id)):
            launch_tile(tile_id, correct_layers=correct_layers, new_version_tag=new_version_tag, temp_dir=temp_dir)
    
    
########################################
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to correct the QC layers on FSC products on HAL.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--qsub_all", action='store_true', help='launch all tiles to be processed on nodes')
    group.add_argument("--process_tile", type=str, help='tile id for which all products will be processed')
    parser.add_argument("--correct_layers", action='store_true', help='correct_layers')
    parser.add_argument("--new_version_tag", type=str, help='new_version_tag')
    parser.add_argument("--temp_dir", type=str, help='temp directory')
    args = parser.parse_args()
    
    if args.qsub_all:
        launch_all(correct_layers=args.correct_layers, new_version_tag=args.new_version_tag, temp_dir=args.temp_dir)
    else:
        process_tile(args.process_tile, correct_layers=args.correct_layers, new_version_tag=args.new_version_tag, temp_dir=args.temp_dir)

