#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess, time



def is_compressible_image_file(filename):
    filename_split_dot = filename.split('.')
    if len(filename_split_dot) < 2:
        return False
    if any([len(el.replace(' ',''))==0 for el in filename_split_dot[-2:]]):
        return False
    if filename_split_dot[-1] not in ['jpg', 'jpeg', 'png']:
        return False
    return True
        
        
def clean_filename(filename):
    filename_split_dot = filename.split('.')
    return '.'.join(filename_split_dot[0:-1]).replace("'",'').replace('.','_').replace('/','_').replace(' ','') + '.' + filename_split_dot[-1]
    


def compress_file(filename, filename_out=None, comp_ratio=20, verbose=0):
    
    #check inputs
    assert isinstance(comp_ratio, int)
    assert comp_ratio > 1
    filename = os.path.abspath(filename)
    assert os.path.exists(filename)
    if not is_compressible_image_file(filename):
        print('file %s does not appear to be a compressible image'%filename)
        return
    if filename_out is not None:
        filename_out = os.path.abspath(filename_out)
        assert is_compressible_image_file(filename_out)
        assert filename_out.split('.')[-1] == filename.split('.')[-1], 'output file cannot have a different extension than input file'
    else:
        filename_out = '%s_compressed%d.%s'%('.'.join(filename.split('.')[0:-1]), comp_ratio, filename.split('.')[-1])
    assert filename_out != filename, 'input and output files cannot be the same'
        
    #make output directory if necessary
    os.makedirs(os.path.dirname(os.path.abspath(filename_out)), exist_ok=True) #make output folder if it does not exist
    
    subprocess.call('unset LD_LIBRARY_PATH; ffmpeg -loglevel panic -nostats -i "%s" -q:v %d "%s"'%(filename, comp_ratio, filename_out), shell=True)
        
    if verbose > 0:
        print('%s -> %s (compressed by ratio %d)'%(filename, filename_out, comp_ratio))
    

def shell_search_files(input_folder, keyword=None):
    cmd = "find %s -type f"%input_folder.replace(' ','\ ').replace('(','\(').replace(')','\)')
    if keyword is not None:
        cmd += " -iname '%s'"%keyword
    files_found = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')
    files_found = [os.path.abspath(el) for el in files_found if os.path.exists(el)]
    return files_found
    

def compress_files_in_folder(input_folder, compressed_folder=None, comp_ratio=20, min_size_on_disk_for_compression=None, duplicate_non_compressible_files=False):
    
    if min_size_on_disk_for_compression is None:
        min_size_on_disk_for_compression = 0.
        
    input_folder = os.path.abspath(input_folder)
    if compressed_folder is None:
        compressed_folder = input_folder + '_compressed_images'
    compressed_folder = os.path.abspath(compressed_folder)
    os.makedirs(compressed_folder, exist_ok=True)
    if len(os.listdir(compressed_folder)):
        raise Exception('output folder is not empty, aborting...')

    
    input_files = shell_search_files(input_folder)
    for input_file in input_files:
        output_file = input_file.replace(input_folder, compressed_folder)
        if is_compressible_image_file(input_file):
            if os.path.getsize(input_file)/1.e6 >= min_size_on_disk_for_compression:
                try:
                    compress_file(input_file, filename_out=output_file, comp_ratio=comp_ratio, verbose=1)
                except:
                    raise
                    print('Compression failed for file %s, copying it to %s instead of compressing it'%(input_file, output_file))
                    shutil.copy(input_file, output_file)
                continue
        if duplicate_non_compressible_files:
            os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
            shutil.copy(input_file, output_file)



if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="compress jpeg images using ffmpeg", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--input", type=str, required=True, help="input path")
    parser.add_argument("--output", type=str, help="output path")
    parser.add_argument("--compression_ratio", type=int, default=20, help="ffmpeg compression ratio (default is 20).")
    parser.add_argument("--min_size_on_disk_for_compression", type=float, help='minimum image size to activate compression in MB. ' + \
        'This is to avoid recompressing already compressed images.')
    parser.add_argument("--duplicate_non_compressible_files", action='store_true', help='if input is folder, than make a complete copy of input folder by duplicating ' + \
        'non image file instad of just generating outputs for image files.')
    args = parser.parse_args()
    

    if os.path.isdir(args.input):
        compress_files_in_folder(args.input, compressed_folder=args.output, comp_ratio=args.compression_ratio, \
            min_size_on_disk_for_compression=args.min_size_on_disk_for_compression, duplicate_non_compressible_files=args.duplicate_non_compressible_files)
    else:
        compress_file(args.input, filename_out=args.output, comp_ratio=args.compression_ratio)


        
    
    
    
