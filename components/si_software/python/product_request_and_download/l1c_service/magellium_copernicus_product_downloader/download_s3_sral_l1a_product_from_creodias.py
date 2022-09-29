#!/usr/bin/env python3
# -*- coding: utf-8 -*-



######################
#developped by Magellium SAS
#author remi.jugier@magellium.fr
######################


import os, sys, shutil, subprocess
from datetime import datetime, timedelta
import time
import requests
import tempfile
import multiprocessing


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
    

def features_to_dict(features):
    return {feature['properties']['title']: convert_dates_in_creodias_dict(feature['properties']) for feature in features}
    
    



class CreodiasS3ProductParser:
    
    
    def __init__(self, username, password):
        self.__username = username
        self.__password = password
        
    
    def __get_token__(self):
        return get_creodias_token(self.__username, self.__password)
        
        
    def __creodias_adress__(self, adress_id, token=None):
        if token is None:
            token = self.__get_token__()
        return '%s?token=%s'%(adress_id, self.__get_token__())
        
        
    def download(self, product_id, output_dir, temp_dir=None, amalthee_timeout=60., creodias_token=None, verbose=1):
        
        start_time = time.time()
        
        #check that product id indeed looks like a S3 SRAL SRA_A product ID, ex : S3B_SR_1_SRA_A__20201116T220021_20201116T225050_20201117T142118_3029_045_371______LN3_O_ST_004.SEN3
        assert 'SRA_A' in product_id
        assert len(product_id.split('.')) == 2
        assert product_id.split('.')[-1] == 'SEN3'
        
        output_filepath = os.path.join(output_dir, product_id)
        
        #check if product was already downloaded
        if os.path.exists(output_filepath):
            return {'status': 'already_downloaded'}
        
        os.makedirs(output_dir, exist_ok=True)
        if temp_dir is None:
            temp_dir = output_dir
            
        #get product information from DIAS API
        info_product = self.get_product_info(product_id, verbose=0, error_mode=False)
        if len(list(info_product.keys())) == 0:
            return {'status': 'unavailable'}
        
        

        #dias_external_api download
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
                output_filepath = '%s/%s'%(output_dir, files_temp[0])
                shutil.move(os.path.join(temp_dir_session, files_temp[0]), output_filepath)
                shutil.rmtree(temp_dir_session)
                if verbose > 0:
                    print('Product successfully downloaded from DIAS API: %s (%s seconds)'%(output_filepath, (time.time()-start_time)))
                status = 'success'
                break
            except Exception as exe:
                print(str(exe))
                if os.path.exists(temp_dir_session):
                    shutil.rmtree(temp_dir_session)
                if os.path.exists(output_filepath):
                    shutil.rmtree(output_filepath)
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
                if os.path.exists(output_filepath):
                    shutil.rmtree(output_filepath)
                raise
                        
            return {'status': status}
                        

            
        
            


    def get_product_info(self, product_list, verbose=0, error_mode=True):
        
        monooutput = False
        if isinstance(product_list, str):
            product_list = [product_list]
            monooutput = True
        assert isinstance(product_list, list)
        assert len(product_list) > 0
        time0 = time.time()
        
        main_url = 'https://finder.creodias.eu/resto/api/collections/Sentinel3/search.json'
        dico = dict()
        pool = multiprocessing.Pool(min(20, len(product_list)))
        for dico_request in pool.map(try_multiple_requests, [main_url + '?' + 'productIdentifier=' + '%25' + product_id + '%25' for product_id in product_list]):
            if dico_request is not None:
                dico.update(features_to_dict(dico_request['features']))
        
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




if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="S3 SRAL L1A downloader from creodias API", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--product_id", type=str, required=True, help="product id. ex: S3B_SR_1_SRA_A__20201116T220021_20201116T225050_20201117T142118_3029_045_371______LN3_O_ST_004.SEN3")
    parser.add_argument("--username", type=str, required=True, help="creodias username (account mail adress)")
    parser.add_argument("--password", type=str, required=True, help="creodias password")
    parser.add_argument("--output_dir", type=str, required=True, help="directory to store L1C file")
    parser.add_argument("--temp_dir", type=str, help="directory to be used as a temporary directory (otherwise output directory will be used)")
    args = parser.parse_args()
    
    
    CreodiasS3ProductParser(args.username, args.password).download(args.product_id, args.output_dir, temp_dir=args.temp_dir)

    
