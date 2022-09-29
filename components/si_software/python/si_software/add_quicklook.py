#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
try:
    from osgeo import gdal
except:
    import gdal



def add_quicklook(product_path, sufix, keep_aux=False, reproject_to_wgs84=False):
    
    file_path = os.path.join(product_path, os.path.basename(product_path) + sufix)
    assert os.path.exists(file_path)
    if reproject_to_wgs84:
        file_path_src = file_path.replace('.' + file_path.split('.')[-1], '_wgs84.' + file_path.split('.')[-1])
        assert file_path_src != file_path
        gdal.Warp(file_path_src, file_path, options=gdal.WarpOptions(format='GTiff', dstSRS='EPSG:4326', \
            warpMemoryLimit=4000., resampleAlg='near', creationOptions=['compress=deflate', 'zlevel=4']))
    else:
        file_path_src = file_path
    quicklook_path = os.path.join(product_path, os.path.basename(product_path) + '_QLK.png')
    
    gdal.Translate(quicklook_path, file_path_src, format='png', outputType=gdal.GDT_Byte, width=1000, height=1000, creationOptions=['zlevel=6'])
    if reproject_to_wgs84:
        os.unlink(file_path_src)
    
    quicklook_aux_path = quicklook_path.replace('.png', '.png.aux.xml')
    if not keep_aux:
        if os.path.exists(quicklook_aux_path):
            os.unlink(quicklook_aux_path)
    
    assert os.path.exists(quicklook_path)


        
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="Add a quicklook to a product", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--product_path", type=str, help="path to input si_software product (FSC, RLIE, or PSA for instance)")
    parser.add_argument("--sufix", type=str, help="name of sufix of file to use as input to generate quicklook (_FSCTOC.tif, _RLIE.tif or _PSA.tif for instance)")
    parser.add_argument("--keep_aux", action='store_true', help="keep aux file")
    parser.add_argument("--reproject_to_wgs84", action='store_true', help="reproject_to_wgs84")
    args = parser.parse_args()
    
    add_quicklook(args.product_path, args.sufix, keep_aux=args.keep_aux, reproject_to_wgs84=args.reproject_to_wgs84)
    
    
