#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess
import copy


program_path = os.path.abspath(__file__)

def parent_dir(path_in, level):
    path_loc = copy.deepcopy(path_in)
    for ii in range(level):
        path_loc = os.path.dirname(path_loc)
    return path_loc


if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to make si_software extraction for CNES')
    parser.add_argument("output_targz", type=str, help='output file path')
    args = parser.parse_args()
    
    try:
        assert args.output_targz.split('.')[-2:] == ['tar', 'gz']
    except:
        raise Exception('output targz path must end with .tar.gz, got %s'%args.output_targz)
    args.output_targz = os.path.abspath(args.output_targz)
    
    #remove __pycache__ folders and jit python files in all cosims git subfolders
    os.chdir(parent_dir(program_path, 4))
    subprocess.check_call('find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf', shell=True)
    
    #move just before the cosims root folder
    os.chdir(parent_dir(program_path, 5))
    
    si_software_dir = 'cosims/components/si_software/python'
    si_software_fol_nocopy = ['si_software_part2']
    paths_copy = []
    for el in os.listdir(si_software_dir):
        if el not in si_software_fol_nocopy:
            paths_copy.append(os.path.join(si_software_dir, el))
    paths_copy += ['cosims/components/common/python/util/log_util.py', 'cosims/components/common/python/util/file_util.py']
    print('\n'.join(paths_copy))
    
    cmd = ['tar', "--exclude", "'__pycache__'", '-zcvf', args.output_targz] + paths_copy
    print(' '.join(cmd))
    subprocess.check_call(cmd)
    
        
    
