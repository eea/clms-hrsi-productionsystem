#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
download_through_dias_vm_intermediary uses a service that reads requests for S2 L1Cs or L2As on a bucket and makes them available on the same bucket
This is necessary for reprocessing on HAL because both the standard DIAS retrieval API and the CNES amalthee retrieval service are unrieliable.
The service works in a very rudimentary way, simply by exchanging files on bucket (at adress_share_requests).

Client : 
1) Adds a 'product_ids' file containing the list of L1C or L2A files requested to a new folder at 'adress_share_requests'.
  To ensure that the server does not read an incomplete file, when it finished copying the 'product_ids' file, it copies a 'copyok' file.
  
Server (this service) :
1) The server scans all directories in 'adress_share_requests' :
  1.1) It erases those where L1C or L2A files were retrieved by the client (contains the 'retrieved' file), 
    or those that where the most recent L1C or L2A was made available more than 3 hours ago.
  1.2) It skips directories where no 'copyok' or 'product_ids' file is found
  1.3) In a directory containing a 'copyok' and 'product_ids' file it reads the 'product_ids' file that contains a list of L1C files.
    1.3.1) If the L1C or L2A file already exists and has been properly copied (contains a 'copyok' file), it skips
    1.3.2) Else it copies the L1C or L2A file to the directory from eodata
  
"""

import os, sys, shutil, subprocess
import requests
from datetime import datetime, timedelta
import tempfile
import json

server_ip = '45.130.31.228'
server_port = 65432
server_port_client = 80
adress_share_products = 'bar:reprocessing/system/l1c_exchange'
cosims_identifier = 'McM'

class Rclone:
    
    def __init__(self, rclone_path=None, config_file=None, allow_env_var_override=False):
        
        self.rclone_path = rclone_path
        if self.rclone_path is None:
            self.rclone_path = shutil.which('rclone')
        assert self.rclone_path is not None
        
        self.config_file = config_file
        if self.config_file is not None:
            assert os.path.exists(self.config_file)
            
        self.allow_env_var_override = allow_env_var_override
            
    
    def rclone_cmd(self, config_file=None):
        cmd = [self.rclone_path]
        if config_file is None:
            if self.allow_env_var_override and 'SI_RCLONE_CONFIG_FILE' in os.environ:
                config_file = os.environ['SI_RCLONE_CONFIG_FILE']
            elif self.config_file is not None:
                config_file = self.config_file
        if config_file is not None:
            assert os.path.exists(config_file)
            cmd += ['--config', config_file]
        return cmd


    def listremotes(self, config_file=None):
        listremotes = [el for el in subprocess.check_output(self.rclone_cmd(config_file=config_file) + ['listremotes']).decode('utf-8').split('\n')[0:-1]]
        return listremotes
        

    def check_remote(self, remote_name, check_access=True, config_file=None):
        if remote_name not in self.listremotes(config_file=config_file):
            raise Exception('remote %s unknown'%remote_name)
        if check_access:
            try:
                subprocess.check_output(self.rclone_cmd(config_file=config_file) + ['lsd', remote_name], timeout=5)
            except:
                print('remote %s inaccessible'%remote_name)
                raise
        
        
    def listdir(self, path, dirs_only=False, silent_error=True, config_file=None):
        if dirs_only:
            cmd = 'lsd'
        else:
            cmd = 'lsf'
        try:
            listdir = [el.split()[-1] for el in subprocess.check_output(self.rclone_cmd(config_file=config_file) + [cmd, path]).decode('utf-8').split('\n')[0:-1]]
        except:
            if silent_error:
                listdir = []
            else:
                raise
        return listdir
        
        
    def rm(self, filepath, config_file=None):
        process = subprocess.Popen(' '.join(self.rclone_cmd(config_file=config_file) + ['delete', filepath]), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            print(stdout.decode('utf-8'))
            print(stderr.decode('utf-8'))
            print('error with command :\n%s'%(' '.join(cmd)))
            raise Exception('error with command :\n%s'%(' '.join(cmd)))
        
        
    def rmtree(self, folder, config_file=None):
        process = subprocess.Popen(' '.join(self.rclone_cmd(config_file=config_file) + ['purge', folder]), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            print(stdout.decode('utf-8'))
            print(stderr.decode('utf-8'))
            print('error with command :\n%s'%(' '.join(cmd)))
            raise Exception('error with command :\n%s'%(' '.join(cmd)))
        
    def copy(self, source, target, add_copyokfile=None, config_file=None, use_sync=False, return_mode=False, use_second_copy=True):
        
        try:
            
            rclone_copy_cmd = 'copy'
            if use_sync:
                rclone_copy_cmd = 'sync'
            
            if source is not None:
                #launch copy command
                cmd = self.rclone_cmd(config_file=config_file) + [rclone_copy_cmd, source, target]
                process = subprocess.Popen(' '.join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()
                if process.returncode != 0:
                    go_error = True
                    if 'eodata:' in source:
                        stderr_lines = [el for el in stderr.decode('utf-8').split('\n') if len(el) > 5]
                        acceptable_errors = ['corrupted on transfer', 'belong in directory']
                        if all([any([acceptable_error in line for acceptable_error in acceptable_errors]) for line in stderr_lines]):
                            go_error = False
                    if go_error:
                        print(stdout.decode('utf-8'))
                        print(stderr.decode('utf-8'))
                        print('error with command :\n%s'%(' '.join(cmd)))
                        raise Exception('error with command :\n%s'%(' '.join(cmd)))
                        
                #sometimes rclone does not properly copy some files, this is a hack to make it less probable that it happens (basically we redo a copy)
                if use_second_copy:
                    self.copy(source, target, config_file=config_file, use_sync=use_sync, use_second_copy=False)
                
            if add_copyokfile is not None:
                subprocess.check_call(self.rclone_cmd(config_file=config_file) + ['copy', add_copyokfile, target])
            
            return {'status': 'success'}
            
        except Exception as exe:
            if return_mode:
                return {'status': 'failure', 'error_msg': str(exe)}
            raise exe
            
            
            
def universal_l1c_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    assert product_id.split('_')[1] == 'MSIL1C'
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert [len(el) for el in product_id.split('_')] == [3, 6, 15, 5, 4, 6, 15] #check if syntax matches 'S2A_MSIL1C_20180102T102421_N0206_R065_T32TLR_20180102T123237' format
    _ = datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S') #check that the sensor_start_date can indeed be read
    _ = datetime.strptime(product_id.split('_')[-1], '%Y%m%dT%H%M%S') #check that the publication_date can indeed be read
    return product_id
    
def universal_l2a_sen2cor_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    assert product_id.split('_')[1] == 'MSIL2A'
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert [len(el) for el in product_id.split('_')] == [3, 6, 15, 5, 4, 6, 15] #check if syntax matches 'S2A_MSIL1C_20180102T102421_N0206_R065_T32TLR_20180102T123237' format
    _ = datetime.strptime(product_id.split('_')[-5], '%Y%m%dT%H%M%S') #check that the sensor_start_date can indeed be read
    _ = datetime.strptime(product_id.split('_')[-1], '%Y%m%dT%H%M%S') #check that the publication_date can indeed be read
    return product_id
    

def extract_archive_within_dir(archive_path, extract_dir):
    if archive_path.split('.')[-1] == 'gz':
        tar_option = '-zxf'
    else:
        tar_option = '-xf'
    subprocess.check_call(['tar', '-C', extract_dir, tar_option, archive_path])
    

def download_s2(product_id, output_dir, rclone_path=None, config_file=None, temp_dir=None, verbose=0):
    
    start_time = datetime.utcnow()
    
    os.makedirs(output_dir, exist_ok=True)
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)
    
    if os.path.basename(product_id).split('_')[1] == 'MSIL1C':
        product_id = universal_l1c_id(product_id)
    elif os.path.basename(product_id).split('_')[1] == 'MSIL2A':
        product_id = universal_l2a_sen2cor_id(product_id)
    else:
        raise Exception('product type of %s not identified'%product_id)
    rcone_util = Rclone(rclone_path=rclone_path, config_file=config_file)
    
    if verbose >= 2:
        print('Requesting %s to %s:%s'%(product_id, server_ip, server_port_client))
    response = requests.get(os.path.join('http://%s:%s'%(server_ip, server_port_client), cosims_identifier + product_id)).json()
    if verbose >= 2:
        print('Received response from %s:%s -> %s'%(server_ip, server_port_client, json.dumps(response, indent=2)))
    
    
    if response['status'] == 'bucket':
        os.makedirs(temp_dir, exist_ok=True)
        temp_dir_session = tempfile.mkdtemp(prefix='dl_loc', dir=temp_dir)
        try:
            rcone_util.copy(os.path.join(adress_share_products, response['token'], product_id + '.SAFE.tar'), temp_dir_session)
            extract_archive_within_dir(os.path.join(temp_dir_session, product_id + '.SAFE.tar'), output_dir)
            assert os.path.exists(os.path.join(output_dir, product_id + '.SAFE'))
            assert os.path.isdir(os.path.join(output_dir, product_id + '.SAFE'))
        except:
            if os.path.exists(os.path.join(output_dir, product_id + '.SAFE')):
                shutil.rmtree(os.path.join(output_dir, product_id + '.SAFE'))
            raise
        finally:
            shutil.rmtree(temp_dir_session)
            try:
                rcone_util.rmtree(os.path.join(adress_share_products, response['token']))
            except:
                print('Could not remove remote directory: %s'%os.path.join(adress_share_products, response['token']))
    else:
        if verbose >= 1:
            print('Could not retrieve product %s, got status %s'%(product_id, response['status']))
        return None
    if verbose >= 1:
        print('Product %s downloaded from COSIMS product service through %s protocol in %s'%(product_id, response['status'], datetime.utcnow()-start_time))
            

        
############################
if __name__ == '__main__':
    

    
    import argparse
    parser = argparse.ArgumentParser(description="Download L1C or L2A product", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--product_id", type=str, required=True, help="S2 product ID")
    parser.add_argument("--output_dir", type=str, required=True, help="output directory")
    parser.add_argument("--config_file", type=str, help="rclone config file if different than ~/.config/rclone/rclone.config")
    parser.add_argument("--temp_dir", type=str, help="temp directory")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    args = parser.parse_args()

    download_s2(args.product_id, args.output_dir, config_file=args.config_file, temp_dir=args.temp_dir, verbose=args.verbose)
    
    
    
    
    
    
