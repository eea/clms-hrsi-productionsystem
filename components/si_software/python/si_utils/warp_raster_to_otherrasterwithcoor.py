#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" 
    reprojection of 1 input raster to match another sample raster
    NB: works with GCPs for input raster but not for sample raster
"""

import os, sys, shutil, subprocess
try:
    from osgeo import gdal
except:
    import gdal
from pyproj import CRS


def reproject(source_file_or_ds, gdal_info_dict_dst, target_file, resample_method='near', return_array=False, remove_target_file=False):

    #get coordinate system in proj4 format
    if not isinstance(gdal_info_dict_dst, dict):
        gdal_info_dict_dst = gdal.Info(gdal_info_dict_dst, format='json')
    proj = CRS.from_user_input(gdal_info_dict_dst['coordinateSystem']['wkt']).to_proj4()
    
    corners = str(gdal_info_dict_dst['cornerCoordinates']['lowerLeft'][0]) + ' ' + str(gdal_info_dict_dst['cornerCoordinates']['lowerLeft'][1]) + ' ' + \
        str(gdal_info_dict_dst['cornerCoordinates']['upperRight'][0]) + ' ' + str(gdal_info_dict_dst['cornerCoordinates']['upperRight'][1])
    gdal_warp_cmd = ['gdalwarp', '-overwrite', '-r', resample_method, '-of', 'GTIFF', '-tr', str(gdal_info_dict_dst['geoTransform'][1]), str(gdal_info_dict_dst['geoTransform'][5]), \
        '-te', corners, '-t_srs', '"%s"'%proj, '-co', '"compress=deflate"', '-co', '"zlevel=4"', source_file_or_ds, target_file]
    print(' '.join(gdal_warp_cmd))
    subprocess.check_call(' '.join(gdal_warp_cmd), shell=True)

    if return_array:
        ar_loc = read_band(target_file)
        if remove_target_file:
            os.unlink(target_file)
        return ar_loc
        
        
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="reprojection of 1 input raster to match another sample raster", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--input_raster", type=str, required=True, help="input_raster: input raster that we want to reproject")
    parser.add_argument("--sample_raster", type=str, required=True, help="sample_raster of which we want to match footprint and resolution")
    parser.add_argument("--output_raster", type=str, required=True, help="output_raster : ouput raster file path")
    parser.add_argument("--resample_method", type=str, help="resampleAlg understood by GDAL. default is 'near'", default='near')
    args = parser.parse_args()
    
    reproject(args.input_raster, args.sample_raster, args.output_raster, resample_method=args.resample_method, return_array=False, remove_target_file=False)
    
