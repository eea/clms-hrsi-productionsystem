#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, subprocess


def aux_data_list_files(output_dir):
    os.chdir(output_dir)
    subprocess.call('find . -type f > list_files.txt', shell=True)

    
if __name__ == '__main__':

    
    import argparse
    parser = argparse.ArgumentParser(description="generate list_files.txt in a directory", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("output_dir", type=str, help='output dir')
    args = parser.parse_args()
    
    aux_data_list_files(args.output_dir)



