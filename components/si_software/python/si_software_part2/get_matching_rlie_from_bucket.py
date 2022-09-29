#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_utils.rclone import Rclone
    
    

def search_rlie_products_local(source_dir, start_date=None, end_date=None, tile_id=None):

    product_paths = find_shell(os.path.abspath(source_dir), expr='RLIE_*', is_dir=True, is_file=False)
    product_dict = dict()
    for product_path in product_paths:
        product_loc = os.path.basename(product_path)
        assert len(product_loc.split('_')) == 6
        date_day_loc = datetime.strptime(product_loc.split('_')[1].split('T')[0], '%Y%m%d')
        if start_date is not None:
            if (date_day_loc < start_date):
                continue
        if end_date is not None:
            if (date_day_loc > end_date):
                continue
        if tile_id is not None:
            if tile_id != product_loc.split('_')[3][1:]:
                continue
        product_dict[product_loc] = product_path
        
    return product_dict
    
    

def search_rlie_products_remote(source_dir, start_date=None, end_date=None, tile_id=None, rclone_config=None, verbose=1):

    assert ':' in source_dir
    product_dict = dict()
    rclone_obj = Rclone(config_file=rclone_config)
    for year in rclone_obj.listdir(source_dir, dirs_only=True, silent_error=True):
        
        #simple speed up to avoid iterating over large amounts of subfolders if not necessary
        if start_date is not None:
            if int(year) < start_date.year:
                continue
        if end_date is not None:
            if int(year) > end_date.year:
                continue
            
        for month in rclone_obj.listdir(os.path.join(source_dir, year), dirs_only=True, silent_error=True):
            for day in rclone_obj.listdir(os.path.join(source_dir, year, month), dirs_only=True, silent_error=True):
                
                date_day = datetime(int(year), int(month), int(day))
                if verbose >= 2:
                    print('Searching %s'%date_day.strftime('%Y-%m-%d'))
                #date selection
                if start_date is not None:
                    if date_day < start_date:
                        continue
                if end_date is not None:
                    if date_day > end_date:
                        continue
                    
                #product selection
                for product_loc in rclone_obj.listdir(os.path.join(source_dir, year, month, day), dirs_only=True, silent_error=True):
                    if 'RLIE_' not in product_loc:
                        continue
                    assert len(product_loc.split('_')) == 6
                    assert datetime.strptime(product_loc.split('_')[1].split('T')[0], '%Y%m%d') == date_day
                    if tile_id is not None:
                        if tile_id != product_loc.split('_')[3][1:]:
                            continue
                    product_dict[product_loc] = os.path.join(source_dir, year, month, day, product_loc)
                    
    return product_dict
    

def select_products_per_tile_day(product_dict):
    day_tile_tags = dict()
    for prod_loc in product_dict.keys():
        date_loc = datetime.strptime(os.path.basename(prod_loc).split('_')[1].split('T')[0], '%Y%m%d')
        tile_id = prod_loc.split('_')[3][1:]
        day_tile_tag_loc = (tile_id, date_loc)
        if day_tile_tag_loc in day_tile_tags:
            previous_product = os.path.basename(day_tile_tags[day_tile_tag_loc])
            previous_product_version = int(previous_product.split('_')[-2][1:])
            current_product_version = int(prod_loc.split('_')[-2][1:])
            if current_product_version < previous_product_version:
                continue
            if current_product_version == previous_product_version:
                if previous_product.split('_')[-1] > prod_loc.split('_')[-1]:
                    continue
                if previous_product.split('_')[-1] == prod_loc.split('_')[-1]:
                    #rare case where S2 product was splitted in 2 by ESA and therefore 2 S2 product exist with very close measurement dates : choose the latest arbitrarily
                    if datetime.strptime(previous_product.split('_')[-5], '%Y%m%dT%H%M%S') > datetime.strptime(prod_loc.split('_')[-5], '%Y%m%dT%H%M%S'):
                        continue
        day_tile_tags[day_tile_tag_loc] = product_dict[prod_loc]
    return day_tile_tags



def get_matching_rlie_from_bucket(input_ref, input_search, output_dir, start_date=None, end_date=None, tile_id=None, download=False, rclone_config=None, verbose=1):
    
    os.makedirs(output_dir, exist_ok=True)
    ref_remote = ':' in input_ref
    search_remote = ':' in input_search
    
    
    #search input_ref
    if verbose >= 1:
        print('Getting ref products in %s'%input_ref)
    if ref_remote:
        dico_ref = search_rlie_products_remote(input_ref, start_date=start_date, end_date=end_date, tile_id=tile_id, rclone_config=rclone_config, verbose=verbose)
    else:
        dico_ref = search_rlie_products_local(input_ref, start_date=start_date, end_date=end_date, tile_id=tile_id)
    #select only 1 product per (tile, day) : the one with the latest product version and if they have the same product version then the one in nominal mode
    dico_ref_tile_day = select_products_per_tile_day(dico_ref)
    if len(dico_ref_tile_day) == 0:
        print('No matching ref products, exiting...')
        return {}
    sat_ref = os.path.basename(dico_ref_tile_day[list(dico_ref_tile_day.keys())[0]]).split('_')[2][0:-1]
    if verbose >=1:
        print('  -> Found %d %s products'%(len(dico_ref_tile_day), sat_ref))
        
        
    #search input_search
    if verbose >= 1:
        print('Getting search products in %s'%input_search)
    dates_ref = [el[1] for el in dico_ref_tile_day.keys()]
    if search_remote:
        dico_search = search_rlie_products_remote(input_search, start_date=min(dates_ref), end_date=max(dates_ref), tile_id=tile_id, rclone_config=rclone_config, verbose=verbose)
    else:
        dico_search = search_rlie_products_local(input_search, start_date=min(dates_ref), end_date=max(dates_ref), tile_id=tile_id)
    #select only 1 product per (tile, day) : the one with the latest product version and if they have the same product version then the one in nominal mode
    dico_search_tile_day = select_products_per_tile_day(dico_search)
    if len(dico_search_tile_day) == 0:
        print('No matching search products, exiting...')
        return {}
    sat_search = os.path.basename(dico_search_tile_day[list(dico_search_tile_day.keys())[0]]).split('_')[2][0:-1]
    if verbose >=1:
        print('  -> Found %d %s products'%(len(dico_search_tile_day), sat_search))
    assert set([sat_ref, sat_search]) == set(['S1', 'S2'])
    
    
    #get matching and write to rlie_s1_s2_matching.json
    dico_matching = dict()
    tags_matched = sorted(list(set(dico_ref_tile_day.keys()).intersection(set(dico_search_tile_day.keys()))))
    for tag_loc in tags_matched:
        assert os.path.basename(dico_ref_tile_day[tag_loc]).split('_')[2][0:-1] == sat_ref
        assert os.path.basename(dico_search_tile_day[tag_loc]).split('_')[2][0:-1] == sat_search
        tag_txt = '%s_%s'%(tag_loc[0], tag_loc[1].strftime('%Y%m%d'))
        dico_matching[tag_txt] = {sat_ref: dico_ref_tile_day[tag_loc], sat_search: dico_search_tile_day[tag_loc]}
        if verbose >= 1:
            print('%s at %s: %s / %s'%(tag_loc[0], tag_loc[1].strftime('%Y%m%d'), os.path.basename(dico_matching[tag_txt][sat_ref]), os.path.basename(dico_matching[tag_txt][sat_search])))
    with open(os.path.join(output_dir, 'rlie_s1_s2_matching.json'), mode='w') as ds:
        json.dump(dico_matching, ds)
    
    #download products
    if download and (ref_remote or search_remote):
        if verbose >= 1:
            print('Downloading products to %s'%output_dir)
        rclone_obj = Rclone(config_file=rclone_config)
        for tag_txt in dico_matching:
            if ref_remote:
                prod_loc = os.path.basename(dico_matching[tag_txt][sat_ref])
                print('  -> %s'%prod_loc)
                rclone_obj.copy(dico_matching[tag_txt][sat_ref], os.path.join(output_dir, prod_loc))
            if search_remote:
                prod_loc = os.path.basename(dico_matching[tag_txt][sat_search])
                print('  -> %s'%prod_loc)
                rclone_obj.copy(dico_matching[tag_txt][sat_search], os.path.join(output_dir, prod_loc))
    
    return dico_matching

    
########################################
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to find matching RLIE S1 products matching RLIE S2 or vice versa.')
    parser.add_argument("--input_ref", type=str, required=True, help='folder to look for reference RLIE products. Can be local or on bucket matching YYYY/mm/dd/rlie_product structure.')
    parser.add_argument("--start_date", type=str, help='start date in YYYY-mm-dd. If input_ref is a remote path it is useful to keep request times small because it will only scan YYYY/mm/dd folders contained within the period. If input_ref is a local dir then it will time select products.')
    parser.add_argument("--end_date", type=str, help='end date in YYYY-mm-dd (included).')
    parser.add_argument("--tile_id", type=str, help='select only RLIE products matching this S2 tile ID.')
    parser.add_argument("--input_search", type=str, required=True, help='folder to search for matching RLIE products. Can be local or on bucket matching YYYY/mm/dd/rlie_product structure.')
    parser.add_argument("--output_dir", type=str, required=True, help='output directory (local only). Will contain a json file referencing matching products ' + \
        'as well as downloaded RLIE products if --download option is active.')
    parser.add_argument("--download", action='store_true', help='download matching products that are on a bucket.')
    parser.add_argument("--rclone_config", type=str, help='rclone configuration file path')
    parser.add_argument("--verbose", type=int, default=1, help='verbose level')
    args = parser.parse_args()
    
    if args.start_date is not None:    
        args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    if args.end_date is not None:
        args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    get_matching_rlie_from_bucket(args.input_ref, args.input_search, args.output_dir, start_date=args.start_date, end_date=args.end_date, tile_id=args.tile_id, \
        download=args.download, rclone_config=args.rclone_config, verbose=args.verbose)

