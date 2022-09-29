#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from si_common.common_functions import *
from si_software_part2.s1_utils import *
from si_utils.rclone import Rclone



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
    dico = dict()
    with fiona.open(shapefile_path) as ds:
        metadata = ds.meta
        proj_in = metadata['crs_wkt']
        if len(proj_in.replace(' ','').replace('\n','')) == 0:
            raise Exception('No shapefile .proj file: %s\n => coordinate system could not be retrieved from shapefile'%shapefile_path.replace('.shp', '.proj'))
        for inv in list(ds.items()):
            dico[inv[1]['properties']['Name']] = shape(inv[1]['geometry'])
    return dico


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
        xx, yy = footprint_wkt.exterior.coords.xy
        url_params['geometry']='POLYGON((' + ','.join(['%s+%s'%(x_loc, y_loc) for x_loc, y_loc in zip(xx, yy)]) +'))'
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


def search_rlie_s2_products(rlie_s2_folder, s2_tile_id, start_date, end_date, credentials=None, temp_dir=None):
    
    if ':' in rlie_s2_folder:
        assert credentials is not None
    
    if credentials is not None:
        drive_name, ip, access_key_id, secret_access_key = credentials.split()
        assert drive_name == rlie_s2_folder.split(':')[0]
          
        if temp_dir is not None:
            temp_dir = os.path.abspath(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)
        temp_dir_session = tempfile.mkdtemp(prefix='rclone_config_', dir=temp_dir)
        config_file = os.path.join(temp_dir_session, 'rclone.conf')
        
        txt = ['[%s]'%drive_name, \
            'type = s3', \
            'env_auth = false', \
            'access_key_id = %s'%access_key_id, \
            'secret_access_key = %s'%secret_access_key, \
            'endpoint = %s'%ip, \
            'location_constraint = RegionOne']

        with open(config_file, mode='w') as ds:
            ds.write('%s\n'%('\n'.join(txt)))
            
    else:
        temp_dir_session, config_file = None, None
    
    product_list = []
    
    try:
        rclone_obj = Rclone(config_file=config_file)
        for year in rclone_obj.listdir(rlie_s2_folder, dirs_only=True, silent_error=True):
            
            #simple speed up to avoid iterating over large amounts of subfolders if not necessary
            if int(year) < start_date.year:
                continue
            if int(year) > end_date.year:
                continue
                
            for month in rclone_obj.listdir(os.path.join(rlie_s2_folder, year), dirs_only=True, silent_error=True):
                for day in rclone_obj.listdir(os.path.join(rlie_s2_folder, year, month), dirs_only=True, silent_error=True):
                    
                    #date selection
                    if datetime(int(year), int(month), int(day)) < start_date:
                        continue
                    if datetime(int(year), int(month), int(day)) > end_date:
                        continue
                        
                    #product selection
                    for product_loc in rclone_obj.listdir(os.path.join(rlie_s2_folder, year, month, day), dirs_only=True, silent_error=True):
                        if len(product_loc.split('_')) != 6:
                            continue
                        if s2_tile_id in product_loc.split('_')[3]:
                            product_list.append(os.path.join(rlie_s2_folder, year, month, day, product_loc))
                    
    finally:
        if temp_dir_session is not None:
            shutil.rmtree(temp_dir_session)
    
    return product_list
    
    
    

def get_s1grd_matching_rlies2(s2_tile_id, s2_shpfile_path, start_date, end_date, rlie_s2_folder, credentials=None, temp_dir=None):
    
    #get s2 product list
    rlie_s2_list = search_rlie_s2_products(rlie_s2_folder, s2_tile_id, start_date, end_date, credentials=credentials, temp_dir=temp_dir)
    dates_dict_s2 = dict()
    for prod_loc in rlie_s2_list:
        date_loc = datetime.strptime(os.path.basename(prod_loc).split('_')[1].split('T')[0], '%Y%m%d')
        if date_loc in dates_dict_s2:
            previous_product = os.path.basename(dates_dict_s2[date_loc])
            previous_product_version = int(previous_product.split('_')[-2][1:])
            current_product_version = int(prod_loc.split('_')[-2][1:])
            if current_product_version < previous_product_version:
                continue
            if current_product_version == previous_product_version:
                assert prod_loc.split('_')[-1] != previous_product.split('_')[-1]
                if prod_loc.split('_')[-1] != '1':
                    continue
        dates_dict_s2[date_loc] = prod_loc
    
    #get s1 product list
    s2tile_footprint = tile_and_polygons(s2_shpfile_path)[s2_tile_id]
    dico_s1 = search_creodias_catalog_image('S1_GRD', start_date, end_date, footprint_wkt=s2tile_footprint, verbose=1)
    grid_s1_list = list(dico_s1.keys())
    dates_dict_s1 = dict()
    for prod_loc in list(dico_s1.keys()):
        date_loc = datetime.strptime(os.path.basename(prod_loc).split('_')[4].split('T')[0], '%Y%m%d')
        if date_loc not in dates_dict_s1:
            dates_dict_s1[date_loc] = []
        dates_dict_s1[date_loc].append(prod_loc)
    
    #print matching dates
    for date_loc, s2_product in dates_dict_s2.items():
        if date_loc in dates_dict_s1:
            print('%s:\n - RLIE_S2: %s\n - S1_GRD:\n%s\n'%(date_loc.strftime('%Y-%m-%d'), s2_product, '\n'.join(['  + %s'%el for el in dates_dict_s1[date_loc]])))
    
    

    
########################################
if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to launch RLIE generation from S1 + S2 products.')
    parser.add_argument("--s2_tile_id", type=str, required=True, help='s2 tile ID')
    parser.add_argument("--s2_shpfile_path", type=str, required=True, help='path to a shapefile with tiles id and corresponding polygons')
    parser.add_argument("--start_date", type=str, required=True, help='start date in YYYY-mm-dd')
    parser.add_argument("--end_date", type=str, required=True, help='end date in YYYY-mm-dd')
    parser.add_argument("--rlie_s2_folder", type=str, required=True, help='RLIE S2 folder containing YYYY/mm/dd subfolders (can be on a bucket but in this case --rclone_config must be filled)')
    parser.add_argument("--credentials", type=str, help='drive_name ip access_key_id secret_access_key (required for use with rlie_s2_folder on a bucket)')
    parser.add_argument("--temp_dir", type=str, help='temporary directory to be used')
    args = parser.parse_args()
        
    args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        
    #main function
    get_s1grd_matching_rlies2(args.s2_tile_id, args.s2_shpfile_path, args.start_date, args.end_date, args.rlie_s2_folder, credentials=args.credentials, temp_dir=args.temp_dir)

