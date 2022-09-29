#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""chain processing on DIAS"""

import os, sys, shutil, subprocess
from datetime import datetime, timedelta


    
def get_product_list(output_file, start_date, end_date, eea39_dir, use_wekeo=False):
    
    from si_report.generate_monthly_report import load_aoi_info
    tile_info = load_aoi_info(eea39_dir)
    
    if not use_wekeo:
        #option 1 : get info frow Copernicus Scihub
        from product_request_and_download.parse_products_copernicus_scihub import CopernicusS2ProductParser
        results = CopernicusS2ProductParser.search(start_date, end_date, footprint_wkt=tile_info['eea39_footprint_wkt'], tile_id_post_selection=tile_info['eea39_tile_list'], \
            properties_selection=['ingestiondate', 'datatakesensingstart'], verbose=0)
    else:
        #option 2 : get info frow Wekeo
        from product_request_and_download.parse_s2_products_wekeo import WekeoS2ProductParser
        results = WekeoS2ProductParser().search(start_date, end_date, tile_id=None, footprint_wkt=tile_info['eea39_footprint_wkt'], tile_id_post_selection=tile_info['eea39_tile_list'], \
            properties_selection=['startDate', 'completionDate', 'updated', 'published', 'productIdentifier'], verbose=0)
    
    product_ids = sorted(list(results.keys()))
    for product_id in product_ids:
        if len(product_id) != 65:
            print(product_id)
    
    if len(os.path.dirname(output_file)) > 0:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, mode='w') as ds:
        ds.write('%s\n'%('\n'.join(product_ids)))
    

    

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="L1C search", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--output_file", type=str, required=True, help="output json file")
    parser.add_argument("--start_date", type=str, required=True, help="start date in %Y-%m-%d format")
    parser.add_argument("--end_date", type=str, required=True, help="end date in %Y-%m-%d format")
    parser.add_argument("--eea39_dir", type=str, required=True, help="path to eea39_dir")
    parser.add_argument("--use_wekeo", action='store_true', help="request to Wekeo instead of ESA")
    args = parser.parse_args()
    
    args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    get_product_list(args.output_file, args.start_date, args.end_date, args.eea39_dir, use_wekeo=args.use_wekeo)


    
