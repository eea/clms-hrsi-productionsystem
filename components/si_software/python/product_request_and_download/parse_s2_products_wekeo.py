#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""chain processing on DIAS"""

from si_common.common_functions import *
import multiprocessing
from si_utils.rclone import Rclone
from si_utils.amalthee_tools import AmaltheeManager


download_modes_list = ['dias_external_api', 'dias_eodata', 'hal_amalthee', 'cosims_l1c_service']


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



def json_http_request_no_error(http_request):
    return json_http_request(http_request, error_mode=False)
    
    
    
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

    
    
        
def convert_dates_in_wekeo_dict(dico):
    for key in dico.keys():
        if key in ['startDate', 'completionDate', 'updated', 'published']:
            str_len = len(dico[key])
            if 22 <= str_len <= 27:
                dico[key] = datetime.strptime(dico[key][0:-1] + '0'*(27-str_len) + dico[key][-1], '%Y-%m-%dT%H:%M:%S.%fZ')
            elif str_len == 20:
                dico[key] = datetime.strptime(dico[key], '%Y-%m-%dT%H:%M:%SZ')
            else:
                raise Exception('%s : invalid wekeo date format'%dico[key])
    return dico
    

def features_to_dict(features, properties_selection=None, tile_id_post_selection=None):
    if tile_id_post_selection is not None:
        if properties_selection is not None:
            return {feature['properties']['title']: convert_dates_in_wekeo_dict({key: feature['properties'][key] for key in properties_selection}) \
                for feature in features if feature['properties']['title'].split('_')[-2][1:] in tile_id_post_selection}
        else:
            return {feature['properties']['title']: convert_dates_in_wekeo_dict(feature['properties']) \
                for feature in features if feature['properties']['title'].split('_')[-2][1:] in tile_id_post_selection}
    else:
        if properties_selection is not None:
            return {feature['properties']['title']: convert_dates_in_wekeo_dict({key: feature['properties'][key] for key in properties_selection}) for feature in features}
        else:
            return {feature['properties']['title']: convert_dates_in_wekeo_dict(feature['properties']) for feature in features}
    
    



class WekeoS2ProductParser:
    
    __default_creodias_username = 'remi.jugier@magellium.fr'
    __default_creodias_password = 'bCLBq4cx5a3WLYL7'
    __l1c_download_modes = download_modes_list
    __eodata_remote = 'eodata:'
    
    def __init__(self, username=None, password=None, rclone_path=None, download_mode=None, rclone_config_file_eodata=None, rclone_config_file_eodata_inter=None):
        self.__username = username
        self.__password = password
        if self.__username is None or self.__password is None:
            self.__username = self.__default_creodias_username
            self.__password = self.__default_creodias_password
        
        #eodata access (on DIAS)
        self.__rclone_path = rclone_path
        try:
            self.__rclone_util_eodata = Rclone(rclone_path=rclone_path, config_file=rclone_config_file_eodata)
            self.__rclone_util_eodata.check_remote(self.__eodata_remote)
        except:
            self.__rclone_util_eodata = None
            
        #amalthee access
        self.__amalthee_manager = AmaltheeManager()
        
        #eodata access (through VM on DIAS intermediary)
        self.__rclone_config_file_eodata_inter = rclone_config_file_eodata_inter
            
        self.download_mode = download_mode
        if self.download_mode is None:
            self.download_mode = self.__get_best_download_solution__()
        assert self.download_mode in self.__l1c_download_modes
        if self.download_mode == 'dias_eodata':
            assert self.__rclone_util_eodata is not None
        elif self.download_mode == 'hal_amalthee':
            assert self.__amalthee_manager is not None

            
    def __get_best_download_solution__(self):
        if self.__rclone_util_eodata is not None:
            return 'dias_eodata'
        elif self.__amalthee_manager.accessible:
            return 'hal_amalthee'
        return 'dias_external_api'
    
    
    def __get_token__(self):
        return get_creodias_token(self.__username, self.__password)

        
        
    def __creodias_adress__(self, adress_id, token=None):
        if token is None:
            token = self.__get_token__()
        return '%s?token=%s'%(adress_id, self.__get_token__())
        
        
    def download(self, product_id, output_dir, download_mode=None, temp_dir=None, amalthee_timeout=60., amalthee_backup_mode=None, creodias_token=None, verbose=1):
        
        start_time = time.time()
        
        if download_mode is None:
            download_mode = self.download_mode
        assert download_mode in self.__l1c_download_modes
        
        #check that product id indeed looks like a L1C product ID
        assert len(product_id.split('.')) == 2
        assert product_id.split('.')[-1] == 'SAFE'
        assert len(product_id.split('_')) == 7
        assert [len(el) for el in product_id.split('_')] == [3, 6, 15, 5, 4, 6, 20]
        
        l1c_filepath = os.path.join(output_dir, product_id)
        
        #check if product was already downloaded
        if os.path.exists(l1c_filepath):
            return {'status': 'already_downloaded'}
        
        os.makedirs(output_dir, exist_ok=True)
        if temp_dir is None:
            temp_dir = output_dir
            
        info_product = self.get_product_info(product_id, properties_selection=['productIdentifier', 'services'], verbose=0, error_mode=False)
        if len(list(info_product.keys())) == 0:
            return {'status': 'unavailable'}
        
        if download_mode == 'dias_eodata':
            #rclone download (must have access to eodata and therefore be on DIAS)
            rclone_src_path = info_product['productIdentifier'].replace('/eodata', 'eodata:EODATA')
            try:
                self.__rclone_util_eodata.copy(rclone_src_path, l1c_filepath)
                if verbose > 0:
                    print('Product successfully downloaded from DIAS EODATA: %s (%s seconds)'%(l1c_filepath, (time.time()-start_time)))
            except:
                if os.path.exists(l1c_filepath):
                    shutil.rmtree(l1c_filepath)
                raise
            return {'status': 'success'}
        
        
        elif download_mode == 'hal_amalthee':
            
            allow_request = False #requests to amalthee take too long, so just take products that are already there
            
            #rclone download (must have access to eodata and therefore be on DIAS)
            amalthee_status, l1c_path_downloaded_amalthee = self.__amalthee_manager.get(product_id, output_dir, timeout=amalthee_timeout, allow_request=allow_request)
            if amalthee_status == 'available':
                if l1c_path_downloaded_amalthee != l1c_filepath:
                    raise Exception('L1C product was downloaded to %s instead of %s'%(l1c_path_downloaded_amalthee, l1c_filepath))
                if verbose > 0:
                    print('Product successfully loaded from CNES datalake (amalthee): %s (%s seconds)'%(l1c_filepath, (time.time()-start_time)))
                return {'status': 'success'}
            elif amalthee_backup_mode is not None:
                assert amalthee_backup_mode in download_modes_list
                if verbose > 0:
                    print('Failed to recover L1C from amalthee service, trying from DIAS L1C service set up by COSIMS (spent %s seconds)'%(time.time()-start_time))
                return self.download(product_id, output_dir, download_mode=amalthee_backup_mode, temp_dir=temp_dir, creodias_token=creodias_token)
            else:
                return {'status': 'failure', 'error_msg': 'Could not retrieve product from amalthee (status=%s).'%amalthee_status}

        

        elif download_mode == 'cosims_l1c_service':
            #download_through_dias_l1c_service set up by COSIMS
            from product_request_and_download.l1c_service.l1c_service_client import download_l1c
            return download_l1c(product_id, output_dir, rclone_path=self.__rclone_path, config_file=self.__rclone_config_file_eodata_inter, temp_dir=temp_dir, verbose=verbose)

            
        elif download_mode == 'dias_external_api':
            if verbose > 0:
                print('trying DIAS HTTP API...')
            #last resort : dias_external_api download, intermittent and unreliable
            ntries = 5
            itry = 0
            status = 'failure'
            temp_dir_session = tempfile.mkdtemp(prefix='tmpdl', dir=temp_dir)
            while(True):
                try:
                    os.makedirs(temp_dir_session, exist_ok=True)
                    adress_id = info_product['services']['download']['url']
                    adress = self.__creodias_adress__(adress_id, token=creodias_token)
                    dl_filename = '%s.zip'%(adress_id.split('/')[-1])
                    dl_filepath = os.path.join(temp_dir_session, dl_filename)
                        
                    #download
                    subprocess.check_call('wget %s -O %s -q'%(adress, dl_filepath), shell=True)

                    #unzip
                    subprocess.check_call('cd %s; unzip -q %s; rm -f %s'%(temp_dir_session, dl_filename, dl_filename), shell=True)
                    files_temp = os.listdir(temp_dir_session)
                    if len(files_temp) != 1:
                        raise Exception('multiple or no files found after unzip')
                    l1c_filepath = '%s/%s'%(output_dir, files_temp[0])
                    shutil.move(os.path.join(temp_dir_session, files_temp[0]), l1c_filepath)
                    shutil.rmtree(temp_dir_session)
                    if verbose > 0:
                        print('Product successfully downloaded from DIAS API: %s (%s seconds)'%(l1c_filepath, (time.time()-start_time)))
                    status = 'success'
                    break
                except Exception as exe:
                    print(str(exe))
                    if os.path.exists(temp_dir_session):
                        shutil.rmtree(temp_dir_session)
                    if os.path.exists(l1c_filepath):
                        shutil.rmtree(l1c_filepath)
                    itry += 1
                    if itry >= ntries:
                        status = 'failure'
                        break
                    else:
                        time.sleep(5.*itry)
                        if verbose > 0:
                            print('  - try %d failed, retrying...'%itry)
                except:
                    if os.path.exists(temp_dir_session):
                        shutil.rmtree(temp_dir_session)
                    if os.path.exists(l1c_filepath):
                        shutil.rmtree(l1c_filepath)
                    raise
                        
            return {'status': status}
                        
        else:
            raise Exception('unknown download mode %s'%download_mode)
            
        
            


    def get_product_info(self, product_list, properties_selection=None, verbose=0, error_mode=True):
        
        monooutput = False
        if isinstance(product_list, str):
            product_list = [product_list]
            monooutput = True
        assert isinstance(product_list, list)
        assert len(product_list) > 0
        time0 = time.time()
        
        main_url = 'https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json'
        dico = dict()
        pool = multiprocessing.Pool(min(20, len(product_list)))
        for dico_request in pool.map(try_multiple_requests, [main_url + '?' + 'productIdentifier=' + '%25' + product_id + '%25' for product_id in product_list]):
            if dico_request is not None:
                dico.update(features_to_dict(dico_request['features'], properties_selection=properties_selection))
        
        if set(product_list) != set(list(dico.keys())):
            print('The following products were not retrieved :\n%s\n'%('\n'.join(['- %s'%el for el in sorted(list(set(product_list) - set(list(dico.keys()))))])))
            print('The following products were retrieved but do not match requested product ids:\n%s\n'%('\n'.join(['- %s'%el for el in sorted(list(set(list(dico.keys())) - set(product_list)))])))
            if error_mode:
                raise Exception('product request and retrieval mismatch')
        
        if verbose > 0:
            print('  -> Executed in %s seconds'%(time.time()-time0))
            
        if monooutput:
            return dico[product_list[0]]
        return dico
        
        
    def get_l1c_list(self, tile_id, date_min, date_max, output_file=None, most_recent_only=True, as_sorted_list=True):
        
        dico = self.search(date_min, date_max, tile_id=tile_id, properties_selection=['productIdentifier'])
        
        #get product list
        l1c_product_dict = dict()
        for l1c_name, info in dico.items():
            assert l1c_name.split('_')[-6] == 'MSIL1C'
            assert l1c_name.split('_')[-2] == 'T' + tile_id
            sensor_start_date = datetime.strptime(l1c_name.split('_')[-5], '%Y%m%dT%H%M%S')
            publication_date = datetime.strptime(l1c_name.split('_')[-1].split('.')[0], '%Y%m%dT%H%M%S')
            if sensor_start_date not in l1c_product_dict:
                l1c_product_dict[sensor_start_date] = dict()
            if most_recent_only:
                if len(l1c_product_dict[sensor_start_date]) == 1:
                    if publication_date < list(l1c_product_dict[sensor_start_date].keys())[0]:
                        continue
                    else:
                        l1c_product_dict[sensor_start_date] = dict()
                elif len(l1c_product_dict[sensor_start_date]) > 1:
                    print(l1c_product_dict[sensor_start_date])
                    raise Exception('most_recent_only: cannot have 2 publication dates already')
            l1c_product_dict[sensor_start_date][publication_date] = l1c_name
            
        #if list required
        if as_sorted_list:
            l1c_product_list = []
            for sensor_start_date in sorted(list(l1c_product_dict.keys())):
                for publication_date in sorted(list(l1c_product_dict[sensor_start_date].keys())):
                    l1c_product_list.append(l1c_product_dict[sensor_start_date][publication_date])
            l1c_product_dict = l1c_product_list
            
        #output file
        if output_file is not None:
            os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
            with open(output_file, mode='w') as ds:
                json.dump(l1c_product_dict, ds, sort_keys=True, indent=4)
                
        return l1c_product_dict
        
    
    def search(self, date_min, date_max, tile_id=None, footprint_wkt=None, tile_id_post_selection=None, properties_selection=None, output_file=None, verbose=0):
        
        assert date_max > date_min
        
        #quick hack to avoid having multiple pages on signle tile id requests : split by years
        if (tile_id is not None) and (date_max - date_min > timedelta(400)):
            date_min_loc, date_max_loc = date_min, min(date_max, date_min+timedelta(400))
            dico = dict()
            while(True):
                dico.update(self.search(date_min_loc, date_max_loc, tile_id=tile_id, footprint_wkt=footprint_wkt, tile_id_post_selection=tile_id_post_selection, \
                    properties_selection=properties_selection, output_file=output_file, verbose=verbose))
                if date_max_loc >= date_max:
                    break
                date_min_loc = date_max_loc
                date_max_loc = min(date_max, date_max_loc+timedelta(400))
            return dico
        
        main_url = 'https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json'
            
        url_params = dict()
        url_params['maxRecords'] = '2000'
        url_params['processingLevel'] = 'LEVEL1C'
        url_params['status'] = 'all'
        url_params['dataset'] = 'ESA-DATASET'
        url_params['sortParam'] = 'published'
        url_params['sortOrder'] = 'descending'
        if tile_id is not None:
            url_params['productIdentifier'] = '%25' + tile_id + '%25'
        if footprint_wkt is not None:
            url_params['geometry'] = '{}'.format(footprint_wkt.replace('POLYGON ','POLYGON').replace(', ',',').replace(' ','+'))
        url_params['startDate'] = date_min.strftime('%Y-%m-%dT%H:%M:%SZ')
        url_params['completionDate'] = date_max.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        full_url = main_url + '?' + '&'.join(['%s=%s'%(key, value) for key, value in url_params.items()])
        dico_request = try_multiple_requests(full_url)
        dico = features_to_dict(dico_request['features'], tile_id_post_selection=tile_id_post_selection, properties_selection=properties_selection)
        
        id_last = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'last']
        assert len(id_last) < 2
        if len(id_last) == 1:
            last_page = int(dico_request['properties']['links'][id_last[0]]['href'].split('=')[-1])+1
            full_url = '='.join(dico_request['properties']['links'][id_last[0]]['href'].split('=')[0:-1]) + '='
            pool = multiprocessing.Pool(max(1,min(20, last_page-1)))
            for dico_request in pool.map(try_multiple_requests, [full_url + str(ii) for ii in range(2,last_page+1)]):
                dico.update(features_to_dict(dico_request['features'], properties_selection=properties_selection))
                
        #in case there was a false count and there are still pages left (happens very often), go through them sequentially
        while(True):
            id_next = [ii for ii in range(len(dico_request['properties']['links'])) if dico_request['properties']['links'][ii]['rel'] == 'next']
            assert len(id_next) < 2
            if len(id_next) == 0:
                break
            elif len(id_next) == 1:
                dico_request = requests.get(dico_request['properties']['links'][id_next[0]]['href']).json()
                dico.update(features_to_dict(dico_request['features'], tile_id_post_selection=tile_id_post_selection, properties_selection=properties_selection))
                
        if output_file is not None:
            dump_json(dico, output_file)
        
        return dico
        
        

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="L1C downloader", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--l1c", type=str, required=True, help="L1C name. ex: S2A_MSIL1C_20200521T102031_N0209_R065_T32TLR_20200521T122533.SAFE")
    parser.add_argument("--output_dir", type=str, required=True, help="directory to store L1C file")
    parser.add_argument("--download_mode", type=str, choices=download_modes_list, default='dias_external_api', help="download modes, default=dias_external_api")
    parser.add_argument("--temp_dir", type=str, help="directory to be used as a temporary directory (otherwise output directory will be used)")
    args = parser.parse_args()
    
    
    WekeoS2ProductParser().download(args.l1c, args.output_dir, temp_dir=args.temp_dir)

    
