#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, subprocess, shutil
try:
    from osgeo import gdal
except:
    import gdal


def create_binary_mask_on_tile_from_shapefiles(raster_model, shapefiles, output_raster, verbose=2):
    
    if not isinstance(shapefiles, list):
        shapefiles = [shapefiles]
        
    #initialize raster
    ds = gdal.Open(raster_model)
    driver = gdal.GetDriverByName('GTiff')
    ds_out = driver.CreateCopy(output_raster, ds, 0)
    inband = ds.GetRasterBand(1)
    outband = ds_out.GetRasterBand(1)
    for i in range(inband.YSize - 1, -1, -1):
        scanline = inband.ReadAsArray(0, i, inband.XSize, 1, inband.XSize, 1)
        scanline = scanline * 0
        outband.WriteArray(scanline, 0, i)
    ds_out = None
    
    #burn each shapefile into the raster
    for shapefile in shapefiles:
        if shapefile is None:
            continue
        cmd = ["gdal_rasterize", "-burn", '1', "-l", os.path.basename(shapefile)[:-4], shapefile, output_raster]
        if verbose > 1:
            print(' '.join(cmd))
        subprocess.check_call(cmd)
        
    #compress raster
    cmd = 'gdal_translate -of GTiff -ot Byte -co tiled=yes -co compress=deflate -co zlevel=4'.split() + [output_raster, output_raster + '_compressed']
    if verbose > 1:
        print(' '.join(cmd))
    subprocess.check_call(cmd)
    shutil.move(output_raster + '_compressed', output_raster)

            
        


if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="Create binary raster using a raster model and shapefiles")
    parser.add_argument("raster_model", type=str, help="Raster model path")
    parser.add_argument("output_raster", type=str, help="Output raster file")
    parser.add_argument("-s", "--shape", type=str, nargs='+', help="Shapefile paths")
    args = parser.parse_args()

    create_binary_mask_on_tile_from_shapefiles(args.raster_model, args.shape, args.output_raster)
