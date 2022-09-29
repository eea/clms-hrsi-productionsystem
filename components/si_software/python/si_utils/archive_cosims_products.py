#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, subprocess, shutil
import tqdm

def find_shell(dir_in, expr=None, is_dir=False, is_file=False):
    if not os.path.exists(dir_in):
        raise Exception('directory %s not found'%dir_in)
    dir_in = os.path.realpath(dir_in)
    cmd = ['find', dir_in]
    if is_dir and is_file:
        raise Exception('is_dir and is_file cannot both be true')
    elif is_dir:
        cmd += ['-type', 'd']
    elif is_file:
        cmd += ['-type', 'f']
    if expr is not None:
        cmd += ['-iname', expr]
    list_find = subprocess.check_output(cmd).decode("utf-8").split('\n')[0:-1]
    for el in list_find:
        if not os.path.exists(el):
            raise Exception('find returned non existing path %s'%el)
        if is_dir and (not os.path.isdir(el)):
            raise Exception('find returned non directory path %s'%el)
        elif (not is_dir) and os.path.isdir(el):
            raise Exception('find returned non file path %s'%el)
    return list_find



def archive_dir(dir_path, archive_path, compress=True):
    if compress:
        tar_option = '-zcf'
    else:
        tar_option = '-cf'
    subprocess.check_call(['tar', '-C', os.path.dirname(dir_path), tar_option, archive_path, os.path.basename(dir_path) + '/'])



def archive_cosims_products(input_dir):
    
    for prefix in ['FSC_', 'RLIE_', 'PSA_']:
        print('\nArchiving %s:'%prefix)
        for dir_path in find_shell(input_dir, expr=prefix + '*', is_dir=True):
            archive_path = dir_path + '.tar'
            archive_dir(dir_path, archive_path, compress=False)
            shutil.rmtree(dir_path)
            print('  -> %s'%archive_path)



if __name__ == '__main__':
    
    
    import argparse
    parser = argparse.ArgumentParser(description="archive cosims products inplace", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("input_dir", type=str, help="input directory.")
    args = parser.parse_args()
    
    archive_cosims_products(args.input_dir)
