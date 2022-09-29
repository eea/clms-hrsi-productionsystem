#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, subprocess
import numpy as np
try:
    from osgeo import gdal
except:
    import gdal

default_cog_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'validate_cloud_optimized_geotiff.py')

def check_file_is_cog(input_file, cog_script=default_cog_script, pixelinfo=False):
    assert os.path.isfile(input_file)
    assert input_file.split('.')[-1] == 'tif'
    subprocess.check_call([args.cog_script, input_file])
    ds = gdal.Open(input_file)
    assert ds.GetRasterBand(1).GetBlockSize() == [256, 256], '%s block size is not right'%input_file
    for ii in range(5):
        assert ds.GetRasterBand(1).GetOverview(ii).GetBlockSize() == [256, 256], '%s block size is not right in overview %d'%(input_file, ii)
    
    if pixelinfo:
        #check pixelinfo
        band = ds.GetRasterBand(1)
        data_ar = np.ma.masked_invalid(band.ReadAsArray())
        nodata_value = band.GetNoDataValue()
        if nodata_value is not None:
            data_ar.mask[data_ar == nodata_value] = True
        ntot = np.prod(np.shape(data_ar))
        nmasked = np.count_nonzero(data_ar.mask)
        print('%d/%d valid pixels (%s%%), nodata value=%s'%(ntot-nmasked, ntot, (ntot-nmasked)*100./(1.*ntot), nodata_value))
        if data_ar.dtype not in [np.float64, np.float32]:
            values = sorted(list(set(data_ar[~data_ar.mask])))
            for val in values:
                nloc = np.count_nonzero(data_ar == val)
                print('  %d: %d/%d (%s%%)'%(val, nloc, ntot, nloc*100./(1.*ntot)))
    

if __name__ == '__main__':
    
    
    import argparse
    parser = argparse.ArgumentParser(description="validate cloud optimized geotiff + block sizes", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("input", type=str, help="path to input product")
    parser.add_argument("--cog_script", type=str, help="path to validate_cog script", default=default_cog_script)
    parser.add_argument("--pixelinfo", action='store_true', help="print stats on pixel values")
    args = parser.parse_args()
    

    if os.path.isdir(args.input):
        for el in sorted(os.listdir(args.input)):
            if '.tif' not in el:
                continue
            check_file_is_cog(os.path.join(args.input, el), cog_script=args.cog_script, pixelinfo=args.pixelinfo)
    elif os.path.isfile(args.input):
        check_file_is_cog(args.input, cog_script=args.cog_script, pixelinfo=args.pixelinfo)
    else:
        raise Exception('%s is not a file nor a dir'%args.input)
