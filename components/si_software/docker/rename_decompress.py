#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, shutil, subprocess
import argparse

def decompress_source(path_in):
    """Decompresses sources automatically if compressed in .tar.gz or .zip, does nothing if it is a folder, exits with error if not an archive or unkown type"""
    src_dir = os.path.dirname(path_in)
    if src_dir == '':
        src_dir = '.'
    files_existing = set(os.listdir(src_dir))
    extension = path_in.split('.')
    if len(extension) == 1:
        return path_in
    if extension[-1] in ['run', 'sh', 'bash']:
        return path_in
    if extension[-1] == 'zip':
        subprocess.check_call(['unzip', '-q', path_in, '-d', src_dir])
        os.unlink(path_in)
        return src_dir + '/' + (set(os.listdir(src_dir)) - files_existing).pop()
    if extension[-1] == 'gz':
        if extension[-2] == 'tar':
            subprocess.check_call(['tar', '-zxf', path_in, '-C', src_dir])
            os.unlink(path_in)
            return src_dir + '/' + (set(os.listdir(src_dir)) - files_existing).pop()
    raise Exception('no extraction method found for path %s'%path_in)



if __name__ == '__main__':
    
    file_dir = os.path.dirname(os.path.realpath(__file__))
    dockerfile = os.path.join(file_dir, 'Dockerfile')
    rename_decompress = os.path.join(file_dir, 'rename_decompress.py')
    default_csi_root_dir = os.path.dirname(os.path.dirname(os.path.dirname(file_dir)))
    
    parser = argparse.ArgumentParser(description='This program search for a single file in the local directory, archive or folder whose lower case name contains search_str and renames it ' +  
        '\(+ decompresses it if necessary) to output_path')
    parser.add_argument("search_str", type=str, help="Search string")
    parser.add_argument("--output_path", type=str, help="Output path, default is search_str")
    args = parser.parse_args()
    
    if args.output_path is None:
        args.output_path = args.search_str
    
    contents = [el for el in os.listdir('.') if args.search_str in el.lower()]
    if len(contents) == 0:
        raise Exception('no match for search_str %s'%args.search_str)
    elif len(contents) > 1:
        raise Exception('multiple match for search_str %s'%args.search_str)
    
    path_decompressed = os.path.realpath(decompress_source(contents[0]))
    shutil.move(path_decompressed, args.output_path)
    
    
