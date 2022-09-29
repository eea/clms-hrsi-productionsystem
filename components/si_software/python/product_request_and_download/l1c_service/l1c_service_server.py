#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from si_common.common_functions import *
from si_utils.rclone import Rclone
from product_request_and_download.l1c_service.common_variables import adress_share_products, server_port, adress_share_products_external, cosims_identifier
import http.server
import threading


    
        
def get_info_from_product_id(product_id):
    
    info = dict()
    
    #Sentinel-2 L1C
    try:
        l1c_id = universal_l1c_id(product_id)
        print('identified L1C: %s'%l1c_id)
    except:
        print('product %s not identified as L1C'%product_id)
        l1c_id = None
    if l1c_id is not None:
        info['type'] = 's2_l1c'
        info['eodata_path'] = 'eodata:EODATA/Sentinel-2/MSI/L1C'
        info['full_product_id'] = l1c_id + '.SAFE'
        info['reference_date'] = datetime.strptime(l1c_id.split('_')[2], '%Y%m%dT%H%M%S')
        return info
    
    #Sentinel-2 L2A
    try:
        l2a_id = universal_l2a_sen2cor_id(product_id)
        print('identified L2A: %s'%l2a_id)
    except:
        print('product %s not identified as L2A'%product_id)
        l2a_id = None
    if l2a_id is not None:
        info['type'] = 's2_l2a'
        info['eodata_path'] = 'eodata:EODATA/Sentinel-2/MSI/L2A'
        info['full_product_id'] = l2a_id + '.SAFE'
        info['reference_date'] = datetime.strptime(l2a_id.split('_')[2], '%Y%m%dT%H%M%S')
        return info
    
    #Sentinel-3 SRAL product
    if '_SR_' in product_id and '.SEN3' in product_id: #sentinel_3 sar product
        assert '/' not in product_id
        assert len(product_id) == 99
        assert len(product_id.split('.')) == 2
        assert product_id.split('.')[-1] == 'SEN3'
        info['reference_date'] = datetime.strptime(product_id[16:31], '%Y%m%dT%H%M%S')
        info['full_product_id'] = product_id
        if product_id[3:15] == '_SR_1_SRA_A_':
            info['type'] = 's3_sral_l1_sraa'
            info['eodata_path'] = 'eodata:EODATA/Sentinel-3/SRAL/SR_1_SRA_A'
            return info
        elif product_id[3:15] == '_SR_1_SRA_BS':
            info['type'] = 's3_sral_l1_srabs'
            info['eodata_path'] = 'eodata:EODATA/Sentinel-3/SRAL/SR_1_SRA_BS'
            return info
        elif product_id[3:15] == '_SR_1_SRA___':
            info['type'] = 's3_sral_l1_sra'
            info['eodata_path'] = 'eodata:EODATA/Sentinel-3/SRAL/SR_1_SRA'
            return info
        elif product_id[3:15] == '_SR_2_LAN___':
            info['type'] = 's3_sral_l2_land'
            info['eodata_path'] = 'eodata:EODATA/Sentinel-3/SRAL/SR_2_LAN'
            return info
        elif product_id[3:15] == '_SR_2_WAT___':
            info['type'] = 's3_sral_l2_ocean'
            info['eodata_path'] = 'eodata:EODATA/Sentinel-3/SRAL/SR_2_WAT'
            return info
        
    raise Exception('could not identify product id')    
        
def system_du(path):
    return int(subprocess.check_output(['du','-s', path]).split()[0].decode('utf-8'))


def store_l1c_on_dias(product_id, rclone_config=None, temp_dir=None, verbose=0, is_cosims=False):
    
    info = get_info_from_product_id(product_id)
    unique_id_subfol = datetime.utcnow().strftime('%Y%m%d%H%H%M%S%f') + '%d'%np.random.randint(10000000)
    temp_dir_session_loc = None
    success = False
    try:
        temp_dir_session_loc = make_temp_dir_session(temp_dir)
        rclone_util = Rclone(config_file=rclone_config)
        
        product_eodata_path = os.path.join(info['eodata_path'], info['reference_date'].strftime('%Y'), info['reference_date'].strftime('%m'), \
            info['reference_date'].strftime('%d'), info['full_product_id'])        
        product_temp_path = os.path.join(temp_dir_session_loc, info['full_product_id'])
        
        try:
            rclone_util.copy(product_eodata_path, product_temp_path)
            if not os.path.exists(product_temp_path):
                print('file %s missing after rclone copy'%(info['full_product_id']))
                raise Exception('L1C missing')
        except:
            print('EODATA request for %s by rclone failed, trying via mounted eodata space'%info['full_product_id'])
            if os.path.exists(product_temp_path):
                shutil.rmtree(product_temp_path)
            assert os.path.exists(product_eodata_path.replace('eodata:EODATA', '/eodata'))
            shutil.copytree(product_eodata_path.replace('eodata:EODATA', '/eodata'), product_temp_path)
            assert system_du(product_temp_path) > 10000
        
        archive_dir(product_temp_path, product_temp_path + '.tar', compress=False)
        if is_cosims:
            share_bucket_path = adress_share_products
        else:
            share_bucket_path = adress_share_products_external
        rclone_util.copy(product_temp_path + '.tar', os.path.join(share_bucket_path, unique_id_subfol))
        success = True
    except:
        unique_id_subfol = None
    finally:
        if temp_dir_session_loc is not None:
            if os.path.exists(temp_dir_session_loc):
                shutil.rmtree(temp_dir_session_loc)
    return success, unique_id_subfol





class ProductOrder:
    
    def __init__(self, product_id, rclone_config=None, temp_dir=None, verbose=0, is_cosims=False):
        self.product_id = product_id
        self.status = 'failed'
        self.token = None
        self.account = None
        self.connexion_start = datetime.utcnow()
        self.connexion_end = None
        self.final_reply = False
        self.__treat_l1c_order(rclone_config=rclone_config, temp_dir=temp_dir, verbose=verbose, is_cosims=is_cosims)
        self.l1c_order_end = datetime.utcnow()
        
    def __treat_l1c_order(self, rclone_config=None, temp_dir=None, verbose=0, is_cosims=False):
            
        #copy L1C from eodata and make it available on the adress_share_products bucket
        success, unique_id_subfol = store_l1c_on_dias(self.product_id, rclone_config=rclone_config, temp_dir=temp_dir, verbose=verbose, is_cosims=is_cosims)
        if success:
            self.status = 'bucket'
            self.token = unique_id_subfol
            
    def get_reply(self):
        if self.status in ['bucket']:
            return {'status': self.status, 'token': self.token}
        return {'status': self.status}

    def __repr__(self):
        return '%s'%self
        
    def __str__(self):
        return dump_json(self.__dict__)
        


class L1CServiceHTTPHandler(http.server.BaseHTTPRequestHandler):
        
    def _set_headers(self, num=200):
        self.send_response(num)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
    def do_HEAD(self):
        self._set_headers()
        
    def do_GET(self):
        
        if self.server.verbose >= 1:
            print('Received connexion from %s:%s with path %s'%(self.client_address[0], self.client_address[1], self.path))
        try:
            self.path = os.path.basename(self.path)
            assert len(self.path) > 10
            is_cosims = self.path[0:3] == cosims_identifier
            if is_cosims:
                info = get_info_from_product_id(self.path[3:])
            else:
                info = get_info_from_product_id(self.path)
        except:
            reply = {'response': 'request is not a valid product ID', 'status': 'unknown_product_id_type'}
            if self.server.verbose >= 2:
                print('Replying to %s:%s -> %s'%(self.client_address[0], self.client_address[1], dump_json(reply)))
            self._set_headers()
            self.wfile.write(dump_json(reply).encode('utf-8'))
            return
        
        product_order = ProductOrder(info['full_product_id'], rclone_config=self.server.rclone_config, temp_dir=self.server.temp_dir, verbose=self.server.verbose, is_cosims=is_cosims)
        reply = product_order.get_reply()
        if self.server.verbose >= 2:
            print('Replying to %s:%s for %s -> %s'%(self.client_address[0], self.client_address[1], info['full_product_id'], dump_json(reply)))
        self._set_headers()
        self.wfile.write(dump_json(reply).encode('utf-8'))
        
    def do_POST(self):
        self._set_headers()
        self.wfile.write(dump_json({'response': 'POST office is closed, mwahahahahaha !', 'status': 'no_post'}).encode('utf-8'))
  
  
class L1CServiceHTTPServer(http.server.ThreadingHTTPServer):
    
    def __init__(self, *args, rclone_config=None, temp_dir=None, verbose=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.threadlock = threading.Lock()
        self.rclone_config = rclone_config
        self.temp_dir = temp_dir
        self.verbose= verbose

    
    
def l1c_service(rclone_config=None, temp_dir=None, verbose=None):
    
    
    if temp_dir is not None:
        os.makedirs(temp_dir, exist_ok=True)
        
        
    threads = []
    server = L1CServiceHTTPServer(('0.0.0.0', server_port), L1CServiceHTTPHandler, rclone_config=rclone_config, temp_dir=temp_dir, verbose=verbose)
    print("Server starts on 0.0.0.0:%s at %s"%(server_port, datetime.utcnow()))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
        server.server_close()
    print("Server stops at %s"%datetime.utcnow())
    
    




if __name__ == '__main__':
            
    import argparse
    parser = argparse.ArgumentParser(description="launch_dias_l1c_service", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--rclone_conf_file", type=str, help="rclone_conf_file path")
    parser.add_argument("--temp_dir", type=str, help="temp_dir")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    args = parser.parse_args()

    l1c_service(rclone_config=args.rclone_conf_file, temp_dir=args.temp_dir, verbose=args.verbose)
    
    
    
