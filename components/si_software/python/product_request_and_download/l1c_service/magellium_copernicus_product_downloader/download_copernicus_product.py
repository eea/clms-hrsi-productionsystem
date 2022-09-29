#!/usr/bin/env python3
# -*- coding: utf-8 -*-


######################
#developped by Magellium SAS
#author remi.jugier@magellium.fr
######################



import os, sys, shutil, subprocess
assert sys.version_info.major >= 3
import requests
from datetime import datetime, timedelta
import tempfile

default_bucket_share_path = 'distribute-external:product_exchange'

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
            

def extract_archive_within_dir(archive_path, extract_dir):
    if archive_path.split('.')[-1] == 'gz':
        tar_option = '-zxf'
    else:
        tar_option = '-xf'
    subprocess.check_call(['tar', '-C', extract_dir, tar_option, archive_path])


def download_copernicus_product(product_id, output_dir, server_ip, server_port, bucket_share_path=None, rclone_path=None, config_file=None, temp_dir=None, overwrite=False, verbose=0):
    
    start_time = datetime.utcnow()
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, product_id)
    if os.path.exists(output_path):
        if overwrite:
            print('Product already exists, removing: %s'%product_id)
            shutil.rmtree(output_path)
        else:
            print('Product already exists, not downloading: %s'%product_id)
            return
    
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)
    
    rcone_util = Rclone(rclone_path=rclone_path, config_file=config_file)
    
    if verbose >= 2:
        print('Requesting %s to %s:%s'%(product_id, server_ip, server_port))
    response = requests.get(os.path.join('http://%s:%s'%(server_ip, server_port), product_id)).json()
    if verbose >= 2:
        print('Received response from %s:%s -> %s'%(server_ip, server_port, dump_json(response)))
    
    if response['status'] == 'bucket':
        os.makedirs(temp_dir, exist_ok=True)
        temp_dir_session = tempfile.mkdtemp(prefix='dl_loc', dir=temp_dir)
        try:
            rcone_util.copy(os.path.join(bucket_share_path, response['token'], product_id + '.tar'), temp_dir_session)
            extract_archive_within_dir(os.path.join(temp_dir_session, product_id + '.tar'), output_dir)
            assert os.path.exists(os.path.join(output_dir, product_id))
            assert os.path.isdir(os.path.join(output_dir, product_id))
        except:
            if os.path.exists(os.path.join(output_dir, product_id)):
                shutil.rmtree(os.path.join(output_dir, product_id))
            raise
        finally:
            shutil.rmtree(temp_dir_session)
            try:
                rcone_util.rmtree(os.path.join(bucket_share_path, response['token']))
            except:
                print('Could not remove remote directory: %s'%os.path.join(bucket_share_path, response['token']))
    else:
        if verbose >= 1:
            print('Could not retrieve L1C %s, got status %s'%(product_id, response['status']))
        return None
    if verbose >= 1:
        print('L1C %s downloaded from COSIMS L1C service through %s protocol in %s'%(product_id, response['status'], datetime.utcnow()-start_time))
            

        
############################
if __name__ == '__main__':
    

    
    import argparse
    parser = argparse.ArgumentParser(description="download_through_dias_l1c_service", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--product_id", type=str, required=True, help="copernicus product ID")
    parser.add_argument("--output_dir", type=str, required=True, help="output directory")
    parser.add_argument("--server_ip", type=str, required=True, help="server ip")
    parser.add_argument("--server_port", type=str, required=True, help="server port")
    parser.add_argument("--bucket_share_path", type=str, default=default_bucket_share_path, help="bucket share path. default: %s"%default_bucket_share_path)
    parser.add_argument("--config_file", type=str, help="rclone config file if different than ~/.config/rclone/rclone.config")
    parser.add_argument("--temp_dir", type=str, help="temp directory")
    parser.add_argument("--overwrite", action='store_true', help="overwrite existing products")
    parser.add_argument("--verbose", type=int, default=1, help="verbose level")
    args = parser.parse_args()

    download_copernicus_product(args.product_id, args.output_dir, args.server_ip, args.server_port, bucket_share_path=args.bucket_share_path, \
        config_file=args.config_file, temp_dir=args.temp_dir, overwrite=args.overwrite, verbose=args.verbose)
    
    
    
    
    
    
