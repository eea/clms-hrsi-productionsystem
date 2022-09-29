#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, shutil
import tempfile
import numpy as np
try:
    from osgeo import gdal
except:
    import gdal
    
    
def get_product_information(fscpath, temp_dir=None):
    
    raise NotImplementedError('not implemented yet!')
    
    #fill information from l2a_path
    mission_fullname = l2a_path.split('/')[-1].split('_')[0]
    if mission_fullname == 'SENTINEL2A':
        product_information['mission'] = 'S2A'
    elif mission_fullname == 'SENTINEL2B':
        product_information['mission'] = 'S2B'
    else:
        raise RuntimeArgError('mission %s recovered from L2A file unknown'%mission_fullname)
        
    #parse L2A xml file
    l2a_xml_path = os.path.join(l2a_path, os.path.basename(l2a_path) + '_MTD_ALL.xml')
    with open(l2a_xml_path) as ds:
        lines = ds.readlines()
    product_information['measurement_date'] = None
    product_information['cloud_cover_percent'] = None
    for line in lines:
        if '<QUALITY_INDEX name="CloudPercent">' in line:
            if product_information['cloud_cover_percent'] is not None:
                raise RuntimeArgError('found <QUALITY_INDEX name="CloudPercent"> multiple times in L2A xml files')
            product_information['cloud_cover_percent'] = float(line.split('</')[0].split('>')[-1])
        elif '<ACQUISITION_DATE>' in line:
            if product_information['measurement_date'] is not None:
                continue
            product_information['measurement_date'] = datetime.strptime(line.split('</')[0].split('>')[-1], '%Y-%m-%dT%H:%M:%S.%fZ')
    assert product_information['measurement_date'] is not None
    assert product_information['cloud_cover_percent'] is not None
    assert abs(product_information['measurement_date'] - datetime.strptime(l2a_path.split('/')[-1].split('_')[1] + '000', '%Y%m%d-%H%M%S-%f')) < timedelta(0,1), \
        'measurement date in L2A xml file does not match date in L2A file name'
        
    #compute shape around data
    product_information['wekeo_geom'] = get_valid_data_convex_hull(os.path.join(l2a_path, 'MASKS', os.path.basename(l2a_path) + '_EDG_R2.tif'), \
        valid_values=[0], proj_out='EPSG:4326', temp_dir=temp_dir).wkt
        
    product_information['tile_name'] = l2a_path.split('/')[-1].split('_')[3][1:]
    assert product_information['product_mode_overide'] in set([0,1]), 'product_mode_overide must be 0 or 1'
    product_information['tag'] = '%s_%s_%s_%s_%d'%(product_information['measurement_date'].strftime('%Y%m%dT%H%M%S'), product_information['mission'], 'T' + product_information['tile_name'], \
        product_information['general_info']['product_version'], product_information['product_mode_overide'])
    
    #fill template information (used to fill FSC, RLIE, PSL and ARLIE templates)
    product_information['template'] = dict()
    #copy information filled by CoSIMS team in the general_info.yaml file
    for key in ['helpdesk_email', 'product_version', 'pum_url', 'dias_url', 'dias_portal_name', 'report_date']:
        product_information['template'][key.upper()] = product_information['general_info'][key]
    for el in ['FSC', 'RLIE']:
        product_information['template']['%s_PRODUCT_ID'%el] = '%s_%s'%(el, product_information['tag'])
    product_information['template']['VALIDATION_REPORT_FILENAME'] = 'hrsi-snow-qar'
    product_information['template']['PRODUCTION_DATE'] = product_information['production_date'].strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    product_information['template']['EDITION_DATE'] = product_information['production_date'].strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    product_information['template']['ACQUISITION_START'] = product_information['measurement_date'].strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    product_information['template']['ACQUISITION_STOP'] = product_information['measurement_date'].strftime('%Y-%m-%dT%H:%M:%S.%f%Z')
    
    #get lonlat border information from the R1.tif file in L2A data
    file_get_coords = ['%s/%s'%(l2a_path, el) for el in os.listdir(l2a_path) if 'R1.tif' in el]
    if len(file_get_coords) == 0:
        raise Exception('Could not find L2A R1.tif file to get tile coordinates')
    lonlat_minmax_dict = RasterPerimeter(file_get_coords[0]).get_lonlat_minmax()
    product_information['template']['WB_lon'] = '%s'%lonlat_minmax_dict['lonmin']
    product_information['template']['EB_lon'] = '%s'%lonlat_minmax_dict['lonmax']
    product_information['template']['SB_lat'] = '%s'%lonlat_minmax_dict['latmin']
    product_information['template']['NB_lat'] = '%s'%lonlat_minmax_dict['latmax']
            
    return product_information

def make_json_for_fsc_product(fscpath, general_info, temp_dir=None):
    if temp_dir is None:
        if 'TMPDIR' in os.environ:
            temp_dir = os.environ['TMPDIR']
        else:
            temp_dir = '.'
    os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(dir=temp_dir, prefix='fscjson_')
    
    
    
    
    
########################################
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to correct the QC layers of a FSC product.')
    parser.add_argument("--fscpath", type=str, required=True, help='path to FSC product')
    parser.add_argument("--general_info", type=str, required=True, help='path to general info yaml file')
    parser.add_argument("--temp_dir", type=str, help='temp dir')
    args = parser.parse_args()
    
    make_json_for_fsc_product(args.fscpath, args.general_info, temp_dir=args.temp_dir)

