#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess
import requests
from datetime import datetime, timedelta
import time
import tempfile
import json
import numpy as np
import fiona
from shapely.geometry import shape, Polygon

default_creodias_credentials = {'user': 'meyijel991@heroulo.com', 'password': 'UQMdjhq5XYS2Crhp'}

def json_http_request(http_request, error_mode=True):
    try:
        output = requests.get(http_request).json()
    except:
        if error_mode:
            raise
        else:
            return None
    return output


def json_http_request_no_error(http_request):
    return json_http_request(http_request, error_mode=False)

def universal_SLC_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert product_id.split('_')[0] in ['S1B', 'S1A']
    assert product_id.split('_')[1] == 'IW'
    assert product_id.split('_')[2] == 'SLC'
    assert [len(el) for el in product_id.split('_')] == [3, 2, 3, 0, 4, 15, 15, 6, 6, 4] #check if syntax matches 'S2A_MSIL1C_20180102T102421_N0206_R065_T32TLR_20180102T123237' format
    _ = datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S') #check that the sensor_start_date can indeed be read
    _ = datetime.strptime(product_id.split('_')[-4], '%Y%m%dT%H%M%S') #check that the publication_date can indeed be read
    product_id += '.SAFE'
    return product_id

def universal_GRDCOG_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert product_id.split('_')[0] in ['S1B', 'S1A']
    assert product_id.split('_')[1] in ['IW', 'EW']
    if product_id.split('_')[1] == 'IW':
        assert product_id.split('_')[2] == 'GRDH'
    elif product_id.split('_')[1] == 'EW':
        assert product_id.split('_')[2] == 'GRDM'
    assert product_id.split('_')[-1] == 'COG'
    assert [len(el) for el in product_id.split('_')] == [3, 2, 4, 4, 15, 15, 6, 6, 4, 3] #check if syntax matches format
    _ = datetime.strptime(product_id.split('_')[-6], '%Y%m%dT%H%M%S') #check that the sensor_start_date can indeed be read
    _ = datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S') #check that the publication_date can indeed be read
    product_id += '.SAFE'
    return product_id


def universal_GRD_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert product_id.split('_')[0] in ['S1B', 'S1A']
    assert product_id.split('_')[1] in ['IW', 'EW']
    if product_id.split('_')[1] == 'IW':
        assert product_id.split('_')[2] == 'GRDH'
    elif product_id.split('_')[1] == 'EW':
        assert product_id.split('_')[2] == 'GRDM'
    assert [len(el) for el in product_id.split('_')] == [3, 2, 4, 4, 15, 15, 6, 6, 4] #check if syntax matches 'S2A_MSIL1C_20180102T102421_N0206_R065_T32TLR_20180102T123237' format
    _ = datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S') #check that the sensor_start_date can indeed be read
    _ = datetime.strptime(product_id.split('_')[-4], '%Y%m%dT%H%M%S') #check that the publication_date can indeed be read
    product_id += '.SAFE'
    return product_id

def get_creodias_token(username, password):
    http_request = "curl -s -d 'client_id=CLOUDFERRO_PUBLIC'" + ' -d "username=%s"'%username + ' -d "password=%s"'%password + \
        " -d 'grant_type=password' 'https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token' --insecure"
    ii = 0
    while(True):
        try:
            out = eval(subprocess.check_output(http_request, shell=True))
            token = out['access_token']
            return token
        except:
            ii += 1
            if ii > 3:
                raise
            time.sleep(5)

def convert_dates_in_creodias_dict(dico):
    for key in dico.keys():
        if key in ['startDate', 'completionDate', 'updated', 'published']:
            str_len = len(dico[key])
            if 22 <= str_len <= 27:
                dico[key] = datetime.strptime(dico[key][0:-1] + '0'*(27-str_len) + dico[key][-1], '%Y-%m-%dT%H:%M:%S.%fZ')
            elif str_len == 20:
                dico[key] = datetime.strptime(dico[key], '%Y-%m-%dT%H:%M:%SZ')
            else:
                raise Exception('%s : invalid creodias date format'%dico[key])
    return dico

def features_to_dict(feature):
    return {feature['properties']['title']: convert_dates_in_creodias_dict(feature['properties']) for feature in feature}

def try_multiple_requests(http_request):
    start_time = time.time()
    ntries, dt = 10, 5.
    i0 = 0
    while(True):
        dico_request = json_http_request_no_error(http_request)
        bad_request = dico_request is None
        if not bad_request:
            if 'features' not in dico_request:
                bad_request = True
        if not bad_request:
            return dico_request
        i0 += 1
        if i0 > ntries:
            break
        time.sleep(dt+np.random.rand())
    if bad_request:
        print(http_request)
        raise Exception('access to DIAS API failure')

def get_product_type(product_id):

    try:
        universal_id = universal_SLC_id(product_id)
        return 'S1_SLC', universal_id
    except:
        pass

    try:
        universal_id = universal_GRD_id(product_id)
        return 'S1_GRD', universal_id
    except:
        pass

    try:
        universal_id = universal_GRDCOG_id(product_id)
        return 'S1_GRDCOG', universal_id
    except:
        pass

    return None

def get_creodias_search_url(product_type):
    if product_type in ['S1_SLC', 'S1_GRD', 'S1_GRDCOG']:
        return 'https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json'

    raise Exception('product type %s is not handled'%product_type)


def get_product_info(product_id, verbose=0):

    if verbose > 0:
        print('Getting product information from creodias for product %s'%product_id)
    time0 = time.time()
    product_type, product_id = get_product_type(product_id)
    if product_type is None:
        raise Exception('product type of product %s is unknown, cannot process'%product_id)
    main_url = get_creodias_search_url(product_type)
    dico_request = try_multiple_requests(main_url + '?' + 'productIdentifier=' + '%25' + product_id + '%25')
    if dico_request is None:
        raise Exception('product %s not found'%product_id)
    dico = features_to_dict(dico_request['features'])
    assert product_id == list(dico.keys())[0], '%s != %s'%(product_id, list(dico.keys())[0])

    if verbose > 0:
        print('  -> Product info gathered in %s seconds'%(time.time()-time0))

    return dico

def tile_to_footprint(tile_id, dictionnary):
    poly=dictionnary[tile_id]
    return poly

def tile_and_polygons(shapefile_path):
    multipolygons = []
    properties = []
    with fiona.open(shapefile_path) as ds:
        metadata = ds.meta
        proj_in = metadata['crs_wkt']
        if len(proj_in.replace(' ','').replace('\n','')) == 0:
            raise Exception('No shapefile .proj file: %s\n => coordinate system could not be retrieved from shapefile'%shapefile_path.replace('.shp', '.proj'))
        for inv in list(ds.items()):
            properties.append(inv[1]['properties']['tile_name'])
            multipolygons.append(shape(inv[1]['geometry']))
    dictio=dict()
    dictio = {properties[0]:multipolygons[0]}
    for i in range (1,len(properties)):
        dictio.update({properties[i]:multipolygons[i]})
    return dictio

def search_creodias_catalog_image(product_type, date_min, date_max, footprint_wkt=None, verbose=0):

    assert date_max > date_min

    #quick hack to avoid having multiple pages on signle tile id requests : split by years
    if (footprint_wkt is not None) and (date_max - date_min > timedelta(400)):
        date_min_loc, date_max_loc = date_min, min(date_max, date_min+timedelta(400))
        dico = dict()
        while(True):
            dico.update(search_creodias_catalog_image(date_min_loc, date_max_loc, footprint_wkt=footprint_wkt,verbose=verbose))
            if date_max_loc >= date_max:
                break
            date_min_loc = date_max_loc
            date_max_loc = min(date_max, date_max_loc+timedelta(400))
        return dico

    main_url = get_creodias_search_url(product_type)

    url_params = dict()
    url_params['maxRecords'] = '2000'
    url_params['startDate'] = date_min.strftime('%Y-%m-%dT%H:%M:%SZ')
    url_params['completionDate'] = date_max.strftime('%Y-%m-%dT%H:%M:%SZ')
    if product_type == 'S1_GRD':
        url_params['productType'] = 'GRD'
    elif product_type == 'S1_GRDCOG':
        url_params['productType'] = 'GRD-COG'
    elif product_type == 'S1_SLC':
        url_params['productType'] = 'SLC'
    else:
        raise Exception('unhandled product type %s'%product_type)
    if footprint_wkt is not None:
        f_wkt=np.zeros((5,2))
        x,y=footprint_wkt.exterior.coords.xy
        xx=[x[0],x[100],x[200],x[300],x[0]]
        yy=[y[0],y[100],y[200],y[300],y[0]]
        for i in range(len(xx)):
            f_wkt[i,0]=xx[i]
            f_wkt[i,1]=yy[i]
        url_params['geometry']='POLYGON((' + str(f_wkt[0,0]) + '+' + str(f_wkt[0,1]) +','+ str(f_wkt[1,0]) + '+' + str(f_wkt[1,1]) +','+ str(f_wkt[2,0]) + '+' + str(f_wkt[2,1]) +','+ str(f_wkt[3,0]) + '+' + str(f_wkt[3,1]) +','+ str(f_wkt[4,0]) + '+' + str(f_wkt[4,1]) +'))'
    url_params['sortParam'] = 'startDate'
    url_params['sortOrder'] = 'descending'
    url_params['status'] = 'all'
    url_params['dataset'] = 'ESA-DATASET'

    full_url = main_url + '?' + '&'.join(['%s=%s'%(key, value) for key, value in url_params.items()])
    dico_request = try_multiple_requests(full_url)
    dico = features_to_dict(dico_request['features'])

    id_last = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'last']
    assert len(id_last) < 2
    if len(id_last) == 1:
        last_page = int(dico_request['properties']['links'][id_last[0]]['href'].split('=')[-1])+1
        full_url = '='.join(dico_request['properties']['links'][id_last[0]]['href'].split('=')[0:-1]) + '='
        pool = multiprocessing.Pool(max(1,min(20, last_page-1)))
        for dico_request in pool.map(try_multiple_requests, [full_url + str(ii) for ii in range(2,last_page+1)]):
            dico.update(features_to_dict(dico_request['features']))

    #in case there was a false count and there are still pages left (happens very often), go through them sequentially
    while(True):
        id_next = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'next']
        assert len(id_next) < 2
        if len(id_next) == 0:
            break
        elif len(id_next) == 1:
            dico_request = requests.get(dico_request['properties']['links'][id_next[0]]['href']).json()
            dico.update(features_to_dict(dico_request['features']))

    return dico

def download_product(product_dict, output_dir, creodias_credentials_u=None, creodias_credentials_p=None, temp_dir=None, verbose=0):

    assert len(product_dict) == 1
    product_id = list(product_dict.keys())[0]
    info_product = list(product_dict.values())[0]

    os.makedirs(output_dir, exist_ok=True)
    product_filepath = os.path.join(output_dir, product_id)
    if os.path.exists(product_filepath):
        if verbose > 0:
            print('product %s already exists, skipping'%product_id)
        return

    if creodias_credentials_u is None and creodias_credentials_p is None :
        creodias_credentials = default_creodias_credentials
    elif creodias_credentials_u is None and creodias_credentials_p is not None :
        raise Exception('please give your username')
    elif creodias_credentials_u is not None and creodias_credentials_p is None :
        raise Exception('please give your password')
    else:
        creodias_credentials = dict()
        creodias_credentials['user'] = creodias_credentials_u
        creodias_credentials['password'] = creodias_credentials_p

    if verbose > 0:
        print('Downloading product %s from creodias'%product_id)
    start_time = time.time()
    #last resort : dias_external_api download, intermittent and unreliable
    ntries = 5
    itry = 0
    status = 'failure'
    temp_dir_session = os.path.abspath(tempfile.mkdtemp(prefix='creodl', dir=temp_dir))
    temp_dir_session_dl = os.path.abspath(tempfile.mkdtemp(prefix='dl', dir=temp_dir_session))
    temp_dir_session_ex = os.path.abspath(tempfile.mkdtemp(prefix='ex', dir=temp_dir_session))
    adress_id = info_product['services']['download']['url']
    dl_filename = '%s.zip'%(adress_id.split('/')[-1])
    dl_filepath = os.path.join(temp_dir_session_dl, dl_filename)

    while(True):
        os.makedirs(temp_dir_session_dl, exist_ok=True)
        try:
            adress = '%s?token=%s'%(adress_id, get_creodias_token(creodias_credentials['user'], creodias_credentials['password']))

            #download
            subprocess.check_call('wget %s -O %s -q'%(adress, dl_filepath), shell=True)

            #unzip
            subprocess.check_call('cd %s; unzip -q %s; rm -f %s'%(temp_dir_session_dl, dl_filename, dl_filename), shell=True)
            files_temp = os.listdir(temp_dir_session_dl)
            if len(files_temp) != 1:
                raise Exception('multiple or no files found after unzip')
            shutil.move(os.path.join(temp_dir_session_dl, files_temp[0]), os.path.join(temp_dir_session_ex, files_temp[0]))
            if verbose > 0:
                print('Product successfully downloaded from DIAS API: %s (%s seconds)'%(files_temp[0], (time.time()-start_time)))
            break
        except:
            itry += 1
            if itry >= ntries:
                raise Exception('  -> download failed after %d tries'%ntries)
            else:
                time.sleep(5.*itry)
                if verbose > 0:
                    print(' -> try %d failed, retrying...'%itry)
        finally:
            shutil.rmtree(temp_dir_session_dl)


    #check that extracted file corresponds to requested product id and move product to output directory
    assert files_temp[0] == product_id
    shutil.move(os.path.join(temp_dir_session_ex, files_temp[0]), product_filepath)
    shutil.rmtree(temp_dir_session)

def download_product_from_creodias(product_id, output_dir, creodias_credentials_u=None, creodias_credentials_p=None, rclone_path=None, config_file=None, temp_dir=None, verbose=0):

    start_time = datetime.utcnow()

    os.makedirs(output_dir, exist_ok=True)
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)

    #get product information
    product_dict = get_product_info(product_id, verbose=verbose)

    #download product
    download_product(product_dict, output_dir, creodias_credentials_u, creodias_credentials_p, temp_dir=temp_dir, verbose=verbose)

def download_product_from_creodias_between_dates(product_type, date_min, date_max, dictionnary, output_dir=None, tile_id=None, creodias_credentials_u=None, creodias_credentials_p=None, \
    temp_dir=None, verbose=0, show_products_only=False):
    
    if not show_products_only:
        assert output_dir is not None
        os.makedirs(output_dir, exist_ok=True)
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)

    if product_type in ['S1_GRD','S1_GRDCOG','S1_SLC']:
        ft_wkt=tile_to_footprint(tile_id,dictionnary)
        dico_products = search_creodias_catalog_image(product_type, date_min, date_max, footprint_wkt=ft_wkt, verbose=verbose)
    else:
        raise Exception('product type %s not handled'%product_type)
        
    print('Products to download:\n%s\n'%('\n'.join(sorted(list(dico_products.keys())))))
    if show_products_only:
        return

    for key in sorted(list(dico_products.keys())):
        download_product({key: dico_products[key]}, output_dir, creodias_credentials_u, creodias_credentials_p, temp_dir=temp_dir, verbose=verbose)

############################
if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="Download satellite products from Creodias", formatter_class=argparse.RawTextHelpFormatter)
    group_ex = parser.add_mutually_exclusive_group(required=True)
    group_ex.add_argument("--product_id", type=str, help="product ID. product type is deduced.")
    group_ex.add_argument("--product_type", type=str, help="product type. can be of types S1_GRD, S1_GRDCOG or S1_SLC. " + \
        "date_min and date_max arguments are required in this case, as well as additional_info in some cases.")
    parser.add_argument("--date_min", type=str, help="date_min in Y-m-d format")
    parser.add_argument("--date_max", type=str, help="date_max in Y-m-d format")
    parser.add_argument("--output_dir", type=str, help="output directory")
    parser.add_argument("--tile_id", type=str, help="tile_id is necessary for product types S2 or S1.")
    parser.add_argument("--creodias_credentials_u", type=str, help="username")
    parser.add_argument("--creodias_credentials_p", type=str, help="password")
    parser.add_argument("--temp_dir", type=str, help="temp directory")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    parser.add_argument("--shpfile_path", type=str, help="path to a shapefile with tiles id and corresponding polygons")
    parser.add_argument("--show_products_only", action='store_true', help="show_products_only, do not download")
    args = parser.parse_args()

    if args.product_id is not None:
        download_product_from_creodias(args.product_id, args.output_dir, temp_dir=args.temp_dir, verbose=args.verbose)
    elif args.product_type is not None:
        dictio=tile_and_polygons(args.shpfile_path)
        assert (args.date_min is not None) and (args.date_max is not None)
        args.date_min = datetime.strptime(args.date_min, '%Y-%m-%d')
        args.date_max = datetime.strptime(args.date_max, '%Y-%m-%d')
        download_product_from_creodias_between_dates(args.product_type, args.date_min, args.date_max, dictio, output_dir=args.output_dir, \
            tile_id=args.tile_id, creodias_credentials_u=args.creodias_credentials_u, creodias_credentials_p=args.creodias_credentials_p,temp_dir=args.temp_dir, \
            verbose=args.verbose, show_products_only=args.show_products_only)
