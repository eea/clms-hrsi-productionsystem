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
    
    



class CreodiasS2GLCProductParser:
    
    
    def __init__(self, username, password):
        self.__username = username
        self.__password = password
        
    
    def __get_token__(self):
        return get_creodias_token(self.__username, self.__password)
        
        
    def __creodias_adress__(self, adress_id, token=None):
        if token is None:
            token = self.__get_token__()
        return '%s?token=%s'%(adress_id, self.__get_token__())
        
        
    def search_creodias_catalog(self, search_tag):
        main_url = 'https://finder.creodias.eu/resto/api/collections/S2GLC/search.json'
        return features_to_dict(try_multiple_requests(main_url + '?' + 'productIdentifier=' + '%25' + search_tag + '%25')['features'])
        
        
    def download(self, search_tag, output_dir, temp_dir=None, verbose=1):
        
        start_time = time.time()
        
        product_info = self.search_creodias_catalog(search_tag)
        if len(product_info) == 0:
            raise Exception('Not product found for search tag %s'%search_tag)
        elif len(product_info) > 1:
            print('products found:\n%s\n'%('\n'.join([' - %s'%product_id for product_id in product_info.keys()])))
            raise Exception('more than 1 product found for search_tag %s'%search_tag)
        
        product_id = list(product_info.keys())[0]
        if verbose > 0:
            print('Getting product %s'%product_id)
        output_filepath = os.path.join(output_dir, product_id)
        
        #check if product was already downloaded
        if os.path.exists(output_filepath):
            if verbose > 0:
                print('  -> Product was already downloaded')
            return {'status': 'success'}
        
        os.makedirs(output_dir, exist_ok=True)
        if temp_dir is None:
            temp_dir = output_dir 

        #dias_external_api download
        ntries = 5
        itry = 0
        status = 'failure'
        temp_dir_session = tempfile.mkdtemp(prefix='tmpdl', dir=temp_dir)
        while(True):

            try:
                os.makedirs(temp_dir_session, exist_ok=True)
                adress_id = product_info[product_id]['services']['download']['url']
                adress = self.__creodias_adress__(adress_id)
                dl_filename = '%s.zip'%(adress_id.split('/')[-1])
                dl_filepath = os.path.join(temp_dir_session, dl_filename)
                
                #download
                subprocess.check_call('wget %s -O %s -q'%(adress, dl_filepath), shell=True)

                #unzip
                subprocess.check_call('cd %s; unzip -q %s; rm -f %s'%(temp_dir_session, dl_filename, dl_filename), shell=True)
                files_temp = os.listdir(temp_dir_session)
                if len(files_temp) != 1:
                    raise Exception('multiple or no files found after unzip')
                output_filepath = os.path.join(output_dir, files_temp[0])
                shutil.move(os.path.join(temp_dir_session, files_temp[0]), output_filepath)
                if verbose > 0:
                    print('  -> Product successfully downloaded from DIAS API: %s (%s seconds)'%(output_filepath, (time.time()-start_time)))
                status = 'success'
                break
                
            except:
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

            finally:
                if os.path.exists(temp_dir_session):
                    shutil.rmtree(temp_dir_session)
                        
        return {'status': status}
            




if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(description="S2GLC downloader from creodias API", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--search_tag", type=str, required=True, help="Either use product ID (ex: S2GLC_T30TWP_2017) or tile ID (ex: 30TWP)")
    parser.add_argument("--username", type=str, required=True, help="creodias username (account mail adress)")
    parser.add_argument("--password", type=str, required=True, help="creodias password")
    parser.add_argument("--output_dir", type=str, required=True, help="directory to store S2GLC file")
    parser.add_argument("--temp_dir", type=str, help="directory to be used as a temporary directory (otherwise output directory will be used)")
    args = parser.parse_args()
    
    
    CreodiasS2GLCProductParser(args.username, args.password).download(args.search_tag, args.output_dir, temp_dir=args.temp_dir)

    
