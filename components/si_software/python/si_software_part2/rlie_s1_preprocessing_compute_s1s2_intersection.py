#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_software_part2.s1_utils import *

def rlie_s1_preprocessing_compute_s1s2_intersection(s1grd_path, s2tiles_eea39_gdal_info, output_json, temp_dir=None):
    
    compute_intersecting_tile_ids_and_geometries(s1grd_path, s2tiles_eea39_gdal_info, output_file=output_json, temp_dir=temp_dir)
    
    
########################################
if __name__ == '__main__':
    
    try:
        import argparse
        parser = argparse.ArgumentParser(description='This script is used to compute intersection between S1 and S2 products and store them into a JSON file. ' + \
            'This file is needed by both pre-processing and processing steps for RLIE S1 product generation.')
        parser.add_argument("--s1grd", type=str, required=True, help='path to S1 GRD .SAFE folder')
        parser.add_argument("--s2tiles_eea39_gdal_info", type=str, required=True, help='path to s2tiles_eea39_gdal_info.json')
        parser.add_argument("--output_json", type=str, required=True, help='path to output JSON file')
        parser.add_argument("--temp_dir", type=str, help='temp_dir')
        args = parser.parse_args()
        
    except Exception as ex:
        print(str(ex))
        sys.exit(exitcodes.wrong_input_parameters)
        
    #main function
    rlie_s1_preprocessing_compute_s1s2_intersection(args.s1grd, args.s2tiles_eea39_gdal_info, args.output_json, temp_dir=args.temp_dir)

