#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess


if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to make unpack and build the Ice script and data for use with COSIMS')
    parser.add_argument("input_zip", type=str, help='input zip file containing ProcessRiverIce and SoftwareData folders')
    parser.add_argument("output_dir", type=str, help='output directory that will contain a ProcessRiverIce.tar.gz archive with compiled ProcessRiverIce software as well as classifiers ' + \
        'and graphs folders + a data folder with HRL files, ARLIE input files and RLIE/ARLIE Metadata files')
    args = parser.parse_args()
    args.input_zip = os.path.abspath(args.input_zip)
    args.output_dir = os.path.abspath(args.output_dir)
    
    if os.path.exists(args.output_dir):
        if len(os.listdir(args.output_dir)) > 0:
            raise Exception('cannot extract to folder %s, it is not empty'%args.output_dir)
    else:
        os.makedirs(args.output_dir)
        
    os.chdir(args.output_dir)
    
    #extract all
    subprocess.check_call(['unzip', args.input_zip])
    
    #everything should be extracted in a main folder containing ProcessRiverIce and SoftwareData folders
    list_extracted = os.listdir('.')
    assert len(list_extracted) == 1
    assert os.path.isdir(list_extracted[0])
    main_ex_fol = os.path.join(os.path.abspath(args.output_dir), list_extracted[0])
    list_extracted = os.listdir(main_ex_fol)
    assert ('ProcessRiverIce.zip' in list_extracted) and ('Software_Data' in list_extracted)
    
    #compile code and copy SNAP classifiers, RLIE graph
    os.chdir(main_ex_fol)
    subprocess.check_call(['unzip', 'ProcessRiverIce.zip'])
    os.chdir('ProcessRiverIce')
    subprocess.check_call(['dotnet', 'publish', '-r', 'linux-x64', '--self-contained', '-o', os.path.join(args.output_dir, 'ProcessRiverIce')])
    os.chdir(os.path.join(args.output_dir, 'ProcessRiverIce'))
    for el in ['classifiers', 'graphs']:
        subprocess.check_call(['unzip', os.path.join(main_ex_fol, 'Software_Data', '%s.zip'%el)])
    os.chdir(args.output_dir)
    subprocess.check_call(['tar', '-zcvf', 'ProcessRiverIce.tar.gz', 'ProcessRiverIce'])
    
    #extract HRL files, ARLIE input files and RLIE/ARLIE Metadata files
    os.makedirs(os.path.join(args.output_dir, 'data'))
    os.chdir(os.path.join(args.output_dir, 'data'))
    for el in ['ARLIE_AOI', 'HRL_FLAGS', 'metadata', 'RiverBasinTiles']:
        subprocess.check_call(['unzip', os.path.join(main_ex_fol, 'Software_Data', '%s.zip'%el)])
        
    
