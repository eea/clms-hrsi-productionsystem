#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess

dirs_copy = ['classifiers', 'graphs', 'metadata', 'TileCodes']

if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to make unpack and build the Ice script and data for use with COSIMS')
    parser.add_argument("input_zip", type=str, help='input ProcessRiverIce zip or dir')
    parser.add_argument("output_dir", type=str, help='output directory that will contain a ProcessRiverIce.tar.gz archive with compiled ProcessRiverIce software as well as classifiers ' + \
        'and graphs folder')
    parser.add_argument("--overwrite", action='store_true', help='overwrite')
    args = parser.parse_args()
    args.input_zip = os.path.abspath(args.input_zip)
    args.output_dir = os.path.abspath(args.output_dir)
    output_ice_src_dir = os.path.join(args.output_dir, 'ProcessRiverIce_src')
    ice_compiled_dirname = 'ProcessRiverIce'
    output_ice_compiled_dir = os.path.join(args.output_dir, ice_compiled_dirname)
    
    current_working_dir = os.getcwd()
    
    if os.path.exists(args.output_dir):
        if args.overwrite:
            shutil.rmtree(args.output_dir)
        elif len(os.listdir(args.output_dir)) > 0:
            raise Exception('cannot extract to folder %s, it is not empty'%args.output_dir)
    os.makedirs(args.output_dir, exist_ok=True)
    
    #extract all
    assert os.path.exists(args.input_zip)
    if os.path.isfile(args.input_zip):
        assert args.input_zip.split('.')[-1] == 'zip'
        os.makedirs(output_ice_src_dir)
        os.chdir(output_ice_src_dir)
        subprocess.check_call(['unzip', args.input_zip])
        os.chdir(current_working_dir)
    else:
        shutil.copytree(input_zip, output_ice_src_dir)
    assert set(dirs_copy).issubset(os.listdir(output_ice_src_dir))
    
    #compile code and copy SNAP classifiers, RLIE graph
    os.chdir(output_ice_src_dir)
    subprocess.check_call(['dotnet', 'publish', '-r', 'linux-x64', '--self-contained', '-o', output_ice_compiled_dir])
    os.chdir(current_working_dir)
    for el in dirs_copy:
        shutil.copytree(os.path.join(output_ice_src_dir, el), os.path.join(output_ice_compiled_dir, el))
    os.chdir(args.output_dir)
    subprocess.check_call(['tar', '-zcvf', 'ProcessRiverIce.tar.gz', ice_compiled_dirname])
    shutil.rmtree(output_ice_src_dir)
    shutil.rmtree(output_ice_compiled_dir)
    
        
    
