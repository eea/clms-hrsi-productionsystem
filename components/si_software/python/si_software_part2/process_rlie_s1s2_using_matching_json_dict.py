#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_software_part2.rlie_s1s2_processing_chain import rlie_s1s2_processing_chain
import yaml


def process_rlie_s1s2_using_matching_json_dict(input_json, dirs_search, output_dir, s2tiles_eea39_gdal_info=None, temp_dir=None, nprocs=None, verbose=None):

    #read json
    with open(input_json) as ds:
        dico_matching = json.load(ds)
        
    #existing_product_paths
    existing_product_paths = []
    for fol in dirs_search:
        existing_product_paths += find_shell(os.path.abspath(fol), expr='RLIE_*', is_dir=True, is_file=False)
    dico_existing_products = dict()
    for prod_path in existing_product_paths:
        dico_existing_products[os.path.basename(prod_path)] = prod_path
        
    #actualize paths in dico_matching
    for key in dico_matching:
        prod_s2 = os.path.basename(dico_matching[key]['S2'])
        prod_s1 = os.path.basename(dico_matching[key]['S1'])
        assert prod_s2 in dico_existing_products, 'product %s not found'%prod_s2
        assert prod_s1 in dico_existing_products, 'product %s not found'%prod_s1
        dico_matching[key] = {'S2': dico_existing_products[prod_s2], 'S1': dico_existing_products[prod_s1]}
        
    #output_dir
    os.makedirs(output_dir, exist_ok=True)
    if temp_dir is not None:
        temp_dir = os.path.abspath(temp_dir)
        os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(prefix='rlies1_', dir=temp_dir)
    
    try:
        output_dir_sub = os.path.join(temp_dir_session, 'out')
        for key in dico_matching:
            
            #run processing
            print('Processing RLIE S1+S2 for %s at %s using %s and %s'%(key.split('_')[0], key.split('_')[1], dico_matching[key]['S1'], dico_matching[key]['S2']))

            if os.path.exists(output_dir_sub):
                shutil.rmtree(output_dir_sub)
            rlie_s1s2_processing_chain(dico_matching[key]['S1'], dico_matching[key]['S2'], output_dir_sub, s2tiles_eea39_gdal_info, \
                temp_dir=temp_dir_session, overwrite=True, timeout=None, nprocs=nprocs, verbose=0, return_with_error_code=False)
            print('  -> Processing successful')
                
            #move products
            with open(os.path.join(output_dir_sub, 'data', 'product_dict.yaml')) as ds:
                product_list = list(yaml.load(ds).values())
            for prod_loc in product_list:
                src_prod, target_prod = os.path.join(output_dir_sub, 'data', prod_loc), os.path.join(output_dir, prod_loc)
                print('%s -> %s'%(src_prod, target_prod))
                shutil.move(src_prod, target_prod)
            
            
    finally:
        shutil.rmtree(temp_dir_session)
        
    
########################################
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to launch RLIE S1+S2 processing on all matching RLIE S1 and S2 products that were identified ' + \
        'through get_matching_rlie_from_bucket.py.')
    parser.add_argument("--input_json", type=str, required=True, help='input json containing dict of matching products')
    parser.add_argument("--dirs_search", type=str, required=True, help='coma separated local paths to look for products')
    parser.add_argument("--output_dir", type=str, required=True, help='output_dir')
    parser.add_argument("--s2tiles_eea39_gdal_info", type=str, required=True, help='path to s2tiles_eea39_gdal_info.json')
    parser.add_argument("--temp_dir", type=str, help='temp_dir')
    parser.add_argument("--nprocs", type=int, default=1, help='nprocs')
    parser.add_argument("--verbose", type=int, default=1, help='verbose level')
    args = parser.parse_args()
    
    args.dirs_search = args.dirs_search.split(',')
    
    process_rlie_s1s2_using_matching_json_dict(args.input_json, args.dirs_search, args.output_dir, \
        s2tiles_eea39_gdal_info=args.s2tiles_eea39_gdal_info, temp_dir=args.temp_dir, nprocs=args.nprocs, verbose=args.verbose)

