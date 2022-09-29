#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil
try:
    from osgeo import gdal
except:
    import gdal
import tempfile
from pdb import set_trace


def rewrite_cog(src_path, dest_path=None, verbose=1):
    
    if os.path.isdir(src_path):
        for root_src, dirs, files in os.walk(src_path, topdown=True):
            root_target = os.path.abspath(root_src).replace(os.path.abspath(src_path), os.path.abspath(dest_path))
            os.makedirs(root_target, exist_ok=True)
            for name in files:
                src_file = os.path.join(root_src, name)
                target_file = os.path.join(root_target, name)
                if os.path.exists(target_file):
                    continue
                if src_file.lower().split('.')[-1] in ['tiff', 'tif']:
                    if verbose > 0:
                        if dest_path is None:
                            print('rewrite_cog: %s (inplace)'%src_file)
                        else:
                            print('rewrite_cog: %s -> %s'%(src_file, target_file))
                    rewrite_cog(src_file, dest_path=target_file, verbose=0)
                elif dest_path is not None:
                    if verbose > 0:
                        print('copy: %s -> %s'%(src_file, target_file))
                    shutil.copy(src_file, target_file)
            for name in dirs:
                target_dir = os.path.join(root_target, name)
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
        return
    
    src_ds = gdal.Open(src_path)
    geo_transform = src_ds.GetGeoTransform()
    proj_src = src_ds.GetProjection()
    band_src = src_ds.GetRasterBand(1)
    data_type = band_src.DataType
    rasterData = band_src.ReadAsArray()
    nodata_val = band_src.GetNoDataValue()
    colortable = band_src.GetRasterColorTable()
    
    dst_ds = gdal.GetDriverByName('MEM').Create('', rasterData.shape[1], rasterData.shape[0], 1, data_type)
    dst_ds.SetGeoTransform(geo_transform)
    dst_ds.SetProjection(proj_src)
    dst_ds.GetRasterBand(1).WriteArray(rasterData)
    if nodata_val is not None:
        dst_ds.GetRasterBand(1).SetNoDataValue(nodata_val)
    band = dst_ds.GetRasterBand(1)
    if colortable is not None:
        band.SetRasterColorTable(colortable)
        band.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)
    gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
    gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '1024')
    dst_ds.BuildOverviews("NEAREST", [2,4,8,16,32])
    band = None
    options = ['COMPRESS=DEFLATE', 'PREDICTOR=1', 'ZLEVEL=4', 'TILED=YES', 'BLOCKXSIZE=1024', 'BLOCKYSIZE=1024', "COPY_SRC_OVERVIEWS=YES"]
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp()
        temp_file = os.path.join(temp_dir, 'temp.tif')
        gdal.GetDriverByName('GTiff').CreateCopy(temp_file, dst_ds, options=options)
        if dest_path is not None:
            if verbose > 0:
                print('rewrite_cog: %s -> %s'%(src_path, dest_path))
            shutil.move(temp_file, dest_path)
        else:
            if verbose > 0:
                print('rewrite_cog: %s (inplace)'%(src_path))
            shutil.move(temp_file, src_path)
    finally:
        if temp_dir is not None:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    del band_src, src_ds


if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="Compress all TIFF files within a folder", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("input", type=str, help="path to input directory or file")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--output", type=str, help="path to output directory or file")
    group.add_argument("--inplace", action='store_true', default=False, help='In place compression in input_folder. This is not recommended since it does not provide ' + \
        'a means to save disk space during processing : it creates all new data in a temporary output_folder and then deletes the input_folder and moves the temporary ' + \
        'output_folder in place of the input_folder. It is therefore only a convenience option. If this option is used and output_folder specified, an error is raised.')
    parser.add_argument("--verbose", type=int, default=1, help="verbose level, default is 1")
    args = parser.parse_args()
    
    if (args.output is None) and (not args.inplace):
        raise Exception('Output is not specified. To transform inplace, use the --inplace option.')
    rewrite_cog(args.input, dest_path=args.output)
    
    
