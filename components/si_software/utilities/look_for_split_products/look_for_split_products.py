#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil, subprocess
from datetime import datetime, timedelta
from calendar import monthrange


default_fsc_bucket_path = 'cosims-results:HRSI/CLMS/Pan-European/High_Resolution_Layers/Snow/FSC'

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



def get_same_day_tile_products(product_list_in, multiple_only=False):
    product_dict = dict()
    for el in product_list_in:
        tile_id = el.split('_')[-3][1:]
        day_str = el.split('_')[-5].split('T')[0]
        tag = (tile_id, day_str)
        if tag in product_dict:
            product_dict[tag].append(el)
        else:
            product_dict[tag] = [el]
    if multiple_only:
        product_dict = {key: value for key, value in product_dict.items() if len(value) >= 2}
    return product_dict



def look_for_split_products(start_date, end_date, fsc_bucket_path=default_fsc_bucket_path):
    
    rclone_obj = Rclone()
    
    product_dict_split_products = dict()
    years = sorted([el[:-1] for el in rclone_obj.listdir(fsc_bucket_path)])
    for year in years:
        year_i = int(year)
        if start_date > datetime(year_i+1,1,1) or end_date < datetime(year_i,1,1):
            continue
        months = sorted([el[:-1] for el in rclone_obj.listdir(os.path.join(fsc_bucket_path, year), silent_error=False)])
        for month in months:
            month_i = int(month)
            if start_date > datetime(year_i, month_i, monthrange(year_i, month_i)[1]) + timedelta(1) or end_date < datetime(year_i, month_i, 1):
                continue
            print('Looking in %02d/%04d...'%(month_i, year_i))
            days = sorted([el[:-1] for el in rclone_obj.listdir(os.path.join(fsc_bucket_path, year, month), silent_error=False)])
            for day in days:
                day_i = int(day)
                if start_date > datetime(year_i, month_i, day_i) + timedelta(1) or end_date < datetime(year_i, month_i, day_i):
                    continue
                product_dict_split_products_loc = get_same_day_tile_products([el[:-1] for el in rclone_obj.listdir(os.path.join(fsc_bucket_path, year, month, day), silent_error=False) \
                    if 'FSC' in el and len(el.split('_')) == 6], multiple_only=True)
                for key, prod_list in product_dict_split_products_loc.items():
                    print('  %s, %s:\n%s'%(key[0], key[1], '\n'.join(['   - %s'%el for el in sorted(prod_list)])))
                product_dict_split_products.update(product_dict_split_products_loc)
    return product_dict_split_products



if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to launch RLIE generation from S1 + S2 products.')
    parser.add_argument("--start_date", type=str, required=True, help='start date in YYYY-mm-dd format')
    parser.add_argument("--end_date", type=str, required=True, help='end date in YYYY-mm-dd format')
    parser.add_argument("--fsc_bucket_path", type=str, default=default_fsc_bucket_path, help='FSC bucket path, default is %s'%default_fsc_bucket_path)
    args = parser.parse_args()
    
    args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    look_for_split_products(args.start_date, args.end_date, fsc_bucket_path=args.fsc_bucket_path)
        
    
