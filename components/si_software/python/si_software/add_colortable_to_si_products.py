#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, shutil
try:
    from osgeo import gdal
except:
    import gdal

def get_unit8_colors_all_nan_transparent():
    colors = gdal.ColorTable()
    for ii in range(255+1):
        colors.SetColorEntry(ii, tuple([0,0,0,0]))
    return colors
    
def add_linspace_colors(colors, values, color_start, color_end):
    nadd = len(values)
    if len(color_start) == 3:
        color_start = color_start + [255]
    if len(color_end) == 3:
        color_end = color_end + [255]
    assert all([0<=ii<=255 for ii in color_start])
    assert all([0<=ii<=255 for ii in color_end])
    for i0, value in enumerate(values):
        coeff = i0*1./(nadd-1.)
        colors.SetColorEntry(value, tuple([int(round(color_start[ii]+coeff*(color_end[ii]-color_start[ii]))) for ii in range(4)]))
    return colors

def add_fsc_colortable(product_path):
    assert os.path.exists(product_path), 'product path %s does not exist'%product_path
    ds = gdal.Open(product_path, 1)
    band = ds.GetRasterBand(1)
    colors = get_unit8_colors_all_nan_transparent()
    colors.SetColorEntry(0, tuple([0,0,0,0]))
    colors = add_linspace_colors(colors, list(range(1,100+1)), [8,51,112], [255,255,255])
    colors.SetColorEntry(205, tuple([123,123,123,255]))
    band.SetRasterColorTable(colors)
    band.FlushCache()
    ds = None
    del ds

def add_rlie_colortable(product_path):
    assert os.path.exists(product_path), 'product path %s does not exist'%product_path
    ds = gdal.Open(product_path, 1)
    band = ds.GetRasterBand(1)
    colors = get_unit8_colors_all_nan_transparent()
    colors.SetColorEntry(1, tuple([0,0,255,255]))
    colors.SetColorEntry(100, tuple([0,232,255,255]))
    colors.SetColorEntry(205, tuple([123,123,123,255]))
    colors.SetColorEntry(254, tuple([255,0,0,255]))
    band.SetRasterColorTable(colors)
    band.FlushCache()
    ds = None
    del ds
    
def add_psa_colortable(product_path):
    assert os.path.exists(product_path), 'product path %s does not exist'%product_path
    ds = gdal.Open(product_path, 1)
    band = ds.GetRasterBand(1)
    colors = get_unit8_colors_all_nan_transparent()
    colors.SetColorEntry(0, tuple([0,0,0,255]))
    colors.SetColorEntry(1, tuple([255,255,255,255]))
    band.SetRasterColorTable(colors)
    band.FlushCache()
    ds = None
    del ds

def add_qc_colortable(product_path, with_cloud=True):
    assert os.path.exists(product_path), 'product path %s does not exist'%product_path
    ds = gdal.Open(product_path, 1)
    band = ds.GetRasterBand(1)
    colors = get_unit8_colors_all_nan_transparent()
    colors.SetColorEntry(0, tuple([93,164,0,255]))
    colors.SetColorEntry(1, tuple([189,189,91,255]))
    colors.SetColorEntry(2, tuple([255,194,87,255]))
    colors.SetColorEntry(3, tuple([255,70,37,255]))
    if with_cloud:
        colors.SetColorEntry(205, tuple([123,123,123,255]))
    band.SetRasterColorTable(colors)
    band.FlushCache()
    ds = None
    del ds
    

    

def add_colortable_to_si_products(product_path, product_tag=None):
    
    success = True
    
    assert os.path.exists(product_path), 'product path %s does not exist'%product_path
    if product_tag is None:
        product_tag = os.path.basename(product_path)
    
    if product_tag.split('_')[0] == 'FSC':
        if os.path.isdir(product_path):
            for el in ['FSCTOC', 'FSCOG', 'QCTOC', 'QCOG', 'NDSI']:
                add_colortable_to_si_products(os.path.join(product_path, '%s_%s.tif'%(product_tag, el)))
        elif product_tag.split('_')[-1] in ['FSCTOC.tif', 'FSCOG.tif', 'NDSI.tif']:
            add_fsc_colortable(product_path)
        elif product_tag.split('_')[-1] in ['QCTOC.tif', 'QCOG.tif']:
            add_qc_colortable(product_path, with_cloud=True)
        else:
            success = False
    elif product_tag.split('_')[0] == 'RLIE':
        if os.path.isdir(product_path):
            for el in ['RLIE', 'QC']:
                add_colortable_to_si_products(os.path.join(product_path, '%s_%s.tif'%(product_tag, el)))
        elif product_tag.split('_')[-1] in ['RLIE.tif']:
            add_rlie_colortable(product_path)
        elif product_tag.split('_')[-1] in ['QC.tif']:
            add_qc_colortable(product_path, with_cloud=True)
        else:
            success = False
    elif product_tag.split('_')[0] == 'PSA':
        if os.path.isdir(product_path):
            for el in ['PSA', 'QC']:
                add_colortable_to_si_products(os.path.join(product_path, '%s_%s.tif'%(product_tag, el)))
        elif product_tag.split('_')[-1] in ['PSA.tif']:
            add_psa_colortable(product_path)
        elif product_tag.split('_')[-1] in ['QC.tif']:
            add_qc_colortable(product_path, with_cloud=False)
        else:
            success = False
        
    if not success:
        print('%s: unidentified product'%product_path)
    return success
    

if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description="", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("product_path", type=str, help="FSC, RLIE or PSA product path. The program identifies which product is concerned py parsing its name.")
    args = parser.parse_args()
    
    add_colortable_to_si_products(args.product_path)
    

    
