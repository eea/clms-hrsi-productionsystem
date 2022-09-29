#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil, subprocess, tempfile
assert sys.version_info.major >= 3


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
    
    
    def search(self, path, expr=None, silent_error=True, config_file=None):
        cmd = self.rclone_cmd(config_file=config_file) + ['ls']
        if expr is not None:
            cmd += ['--include', '"' + expr + '"']
        cmd.append(path)
        temp_dir_session = tempfile.mkdtemp(dir='.', prefix='rclonesearch_')
        try:
            file_temp = os.path.join(temp_dir_session, 'listloc.txt')
            os.system(' '.join(cmd) + ' > %s'%file_temp)
            with open(file_temp) as ds:
                listdir = [el.split()[-1] for el in ds.read().split('\n')[0:-1]]
        except:
            if silent_error:
                listdir = []
            else:
                raise
        finally:
            shutil.rmtree(temp_dir_session)
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
            
