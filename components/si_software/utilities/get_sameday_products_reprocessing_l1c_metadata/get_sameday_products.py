#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil
import json
from datetime import datetime, timedelta


def get_sameday_products(dico_in, n_same_day=2, verbose=0):
    
    if isinstance(dico_in, str):
        dico_in = json.load(open(dico_in))
    assert isinstance(dico_in, dict)
    
    dico_out = dict()
    for key, value in dico_in.items():
        day_loc = datetime.strptime(key.split('_')[2].split('T')[0], '%Y%m%d')
        if day_loc not in dico_out:
            dico_out[day_loc] = dict()
        dico_out[day_loc][key] = value
    
    txt_lines = []
    for day_loc in sorted(list(dico_out.keys())):
        if len(dico_out[day_loc]) < n_same_day:
            continue
        txt_lines_loc = [day_loc.strftime('%Y-%m-%d:')]
        for key in sorted(list(dico_out[day_loc].keys())):
            txt_lines_loc.append(' - %s: SENSING_TIME = %s'%(key, dico_out[day_loc][key]['measurement_date'].replace('datetime:', '')))
        if verbose > 0:
            print('\n'.join(txt_lines_loc))
        txt_lines += txt_lines_loc

    return txt_lines
    
    

def get_sameday_products_dir(input_dir, n_same_day=2):
    
    for el in sorted(os.listdir(input_dir)):
        if '.json' not in el:
            continue
        txt_lines = get_sameday_products(os.path.join(input_dir, el), n_same_day=n_same_day, verbose=0)
        if 'S2B_MSIL1C_20190626T093039_N0207_R136_T35VMD_20190629T151654.SAFE' not in ''.join(txt_lines):
            continue
        if len(txt_lines) > 0:
            print('\n%s:\n%s'%(el, '\n'.join(['  %s'%txt_line for txt_line in txt_lines])))
        
        
    

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="Get sameday products from JSON file", formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--input_json", type=str, help="input JSON file containing {product_id: metadata_dict}")
    group.add_argument("--input_json_dir", type=str, help="input directory containing JSON files {product_id: metadata_dict}")
    parser.add_argument("--n_same_day", type=int, default=2, help="minimum number of products on the same day, default is 2")
    args = parser.parse_args()
    
    if args.input_json is None:
        get_sameday_products_dir(args.input_json_dir, n_same_day=args.n_same_day)
    else:
        get_sameday_products(args.input_json, n_same_day=args.n_same_day, verbose=1)
