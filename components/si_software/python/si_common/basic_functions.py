#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess, stat
import copy, time, glob
from datetime import datetime, timedelta, timezone
from multiprocessing import cpu_count
import threading
import tempfile
import requests
import numpy as np
import tqdm
import dateutil.parser as DateParser
from pdb import set_trace

from si_common.exitcodes import *

coded_assert(sys.version_info.major >= 3, 'python versions 3 and above required', exitcodes.program_internal_test_error)



def timedelta_month(datetime_in, n_month):
    datetime_loc = datetime(datetime_in.year, datetime_in.month, 1, 0, 0, 0)
    timedelta_loc = datetime_in - datetime_loc
    for ii in range(abs(n_month)):
        year, month = datetime_loc.year, datetime_loc.month
        if n_month > 0:
            month += 1
        else:
            month -= 1
        if month == 13:
            year += 1
            month = 1
        elif month == 0:
            year -= 1
            month = 12
        datetime_loc = datetime(year, month, 1, 0, 0, 0)
    return datetime_loc + timedelta_loc
    


def is_remote(path):
    return ':' in path.split('/')[0]

def logical_array_list_operation(list_in, operation):
    assert isinstance(list_in, list)
    assert len(list_in) > 0
    assert operation in ['or', 'and']
    ar_loc = copy.deepcopy(list_in[0])
    for ii in range(1, len(list_in)):
        if operation == 'or':
            ar_loc = np.logical_or(ar_loc, list_in[ii])
        elif operation == 'and':
            ar_loc = np.logical_and(ar_loc, list_in[ii])
    return ar_loc



def find_shell(dir_in, expr=None, is_dir=False, is_file=False):
    if not os.path.exists(dir_in):
        raise Exception('directory %s not found'%dir_in)
    dir_in = os.path.realpath(dir_in)
    cmd = ['find', dir_in]
    if is_dir and is_file:
        raise Exception('is_dir and is_file cannot both be true')
    elif is_dir:
        cmd += ['-type', 'd']
    elif is_file:
        cmd += ['-type', 'f']
    if expr is not None:
        cmd += ['-iname', expr]
    list_find = subprocess.check_output(cmd).decode("utf-8").split('\n')[0:-1]
    for el in list_find:
        if not os.path.exists(el):
            raise Exception('find returned non existing path %s'%el)
        if is_dir and (not os.path.isdir(el)):
            raise Exception('find returned non directory path %s'%el)
        elif (not is_dir) and os.path.isdir(el):
            raise Exception('find returned non file path %s'%el)
    return list_find
    

def simple_read_date(date_str):
    try:
        if date_str[-1] == 'Z':
            date_str = date_str[0:-1]

        if len(date_str) == 10:
            return datetime.strptime(date_str, '%Y-%m-%d')
        elif len(date_str) == 19:
            return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        elif 19 < len(date_str) <= 26:
            return datetime.strptime(date_str + '0' * (26 - len(date_str)), '%Y-%m-%dT%H:%M:%S.%f')

    except:
        print('Failed to retrieve date from: %s' % date_str)
        raise

    raise Exception('date format mismatch: %s' % date_str)


def generic_function(dico):
    if 'args' not in dico:
        dico['args'] = []
    if 'kwargs' not in dico:
        dico['kwargs'] = {}
    dico['function'](*dico['args'], **dico['kwargs'])


class ThreadWithReturnValue(threading.Thread):
    def __init__(self, target, args=(), kwargs={}, daemon=False):
        super().__init__(group=None, target=target, args=args, kwargs=kwargs, daemon=daemon)
        self._return = None

    def run(self):
        self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        return self._return


def archive_dir(dir_path, archive_path, compress=True):
    if compress:
        tar_option = '-zcf'
    else:
        tar_option = '-cf'
    subprocess.check_call(
        ['tar', '-C', os.path.dirname(dir_path), tar_option, archive_path, os.path.basename(dir_path) + '/'])


def extract_archive_within_dir(archive_path, extract_dir):
    if archive_path.split('.')[-1] == 'gz':
        tar_option = '-zxf'
    else:
        tar_option = '-xf'
    subprocess.check_call(['tar', '-C', extract_dir, tar_option, archive_path])


def make_temp_dir_session(temp_dir, prefix='temp_si_soft'):
    if temp_dir is None:
        temp_dir = os.path.abspath(os.getcwd())
    os.makedirs(temp_dir, exist_ok=True)
    temp_dir_session = tempfile.mkdtemp(prefix=prefix, dir=temp_dir)
    return temp_dir_session


def check_dict(dico, keys, check_none=True, prefix=None):
    none_keys = []
    not_present_keys = []
    for key in keys:
        if key not in dico:
            not_present_keys.append(key)
        else:
            if dico[key] is None:
                none_keys.append(key)
    msg = ''
    if len(not_present_keys) > 0:
        msg += 'Missing keys: %s' % (', '.join(not_present_keys))
    if (len(none_keys) > 0) and check_none:
        if len(msg) > 0:
            msg += '\n'
        msg += 'None keys: %s' % (', '.join(none_keys))
    if len(msg) > 0:
        if prefix is not None:
            msg = '%s%s' % (prefix, msg)
        raise CodedException(msg, exitcode=exitcodes.wrong_input_parameters)


def unicity(list_in):
    if len(list_in) == len(set(list_in)):
        return True
    return False


def get_txt_from_httprequest(httprequest, userpass_dict=None, return_mode=None, verbose=0):
    if return_mode is None:
        return_mode = 'txt'
    assert return_mode in ['txt', 'txtlines'], 'return_mode %s unknown' % return_mode
    temp_file = tempfile.mkstemp(dir='.')[1]
    try:
        cmd = ['wget', '' if verbose else '-q', '--no-check-certificate']
        if userpass_dict is not None:
            cmd += ['--user=%s' % userpass_dict['user'], '--password=%s' % userpass_dict['password']]
        cmd += ['--output-document=%s' % temp_file, httprequest]
        if verbose > 0:
            print(' '.join(cmd))
        try:
            subprocess.check_call(' '.join(cmd), shell=True)
        except:
            print('  -> get_txt_from_httprequest: 1st try failed, 2nd try...')
            subprocess.check_call(' '.join(cmd), shell=True)
        with open(temp_file) as ds:
            if return_mode == 'txtlines':
                txt = ds.readlines()
            elif return_mode == 'txt':
                txt = ds.read()
            else:
                raise Exception('return_mode %s unknown' % return_mode)
        os.unlink(temp_file)
        return txt
    except:
        os.unlink(temp_file)
        raise


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


def string2bytes(str_in):
    return str_in.encode('utf-8')


def bytes2string(str_in):
    return str_in.decode('utf-8')


def compute_size_du(path, human_readable=False):
    """disk usage"""
    if human_readable:
        return subprocess.check_output(['du', '-hs', path]).split()[0].decode('utf-8')
    else:
        return subprocess.check_output(['du', '-s', path]).split()[0].decode('utf-8')


def list_form(list_in):
    if isinstance(list_in, list):
        list_out = list_in
    else:
        list_out = [list_in]
    return list_out


def list_argsort(li):
    return sorted(range(len(li)), key=lambda k: li[k])


def is_sorted(li):
    if len(li) <= 1:
        return True
    return all([li[ii] >= li[ii - 1] for ii in range(1, len(li))])


def check_path(path, is_dir=False):
    assert os.path.exists(path), 'path %s does not exist' % path
    if is_dir:
        assert os.path.isdir(path), 'path %s is not a directory' % path
    return path


def create_link(src, dst):
    check_path(src)
    if os.path.exists(dst):
        os.unlink(dst)
    os.symlink(os.path.abspath(src), dst)


def link_all(expr_in, dst_fol):
    if dst_fol[-1] == '/':
        dst_fol = dst_fol[0:-1]
    check_path(dst_fol, is_dir=True)
    for src in glob.glob(expr_in):
        create_link(src, dst_fol + '/' + src.split('/')[-1])


def original_path(file_in):
    if not os.path.islink(file_in):
        return file_in
    return original_path(os.readlink(file_in))


def copy_original(src, dst):
    src_realpath = original_path(src)
    if os.path.isdir(src_realpath):
        shutil.copytree(src_realpath, dst)
    else:
        shutil.copy(src_realpath, dst)


def copy_all(expr_in, dst_fol):
    if dst_fol[-1] == '/':
        dst_fol = dst_fol[0:-1]
    check_path(dst_fol, is_dir=True)
    for src in glob.glob(expr_in):
        copy_original(src, dst_fol + '/' + src.split('/')[-1])


def make_folder(fol, check_empty=True):
    if not os.path.exists(fol):
        os.system('mkdir -p %s' % fol)
        if check_empty:
            return 'empty'
        return 'created'
    if check_empty:
        if (len(os.listdir(fol)) == 0):
            return 'empty'
        else:
            return 'not empty'
    return 'exists'


def search_folder_structure(folder_in, regexp=None, maxdepth=None, object_type=None, case_sensible=False):
    cmd = 'find %s' % folder_in
    if maxdepth is not None:
        cmd += ' -maxdepth %d' % maxdepth
    if object_type is not None:
        assert object_type in ['d', 'f'], 'object type %s unknown' % object_type
        cmd += ' -type ' + object_type
    if regexp is not None:
        if case_sensible:
            cmd += ' -name ' + regexp
        else:
            cmd += ' -iname ' + regexp
    try:
        list_out = subprocess.check_output(cmd, shell=True).decode('utf-8').split('\n')[0:-1]
        return list_out
    except:
        return []


def check_s2tile_name(tile_id):
    if len(tile_id) == 6:
        assert tile_id[0] == 'T', '6 characters S2 tile_id must start with T'
        tile_id = tile_id[1:]
    assert len(tile_id) == 5, 'S2 tile_id must have 5 characters or 6 that start with T'
    assert set(tile_id[0:2]).issubset(set('0123456789')), "tile id %s is not a S2 tile_id" % tile_id
    return tile_id


def check_laeatile_name(tile_id):
    assert len(tile_id) == 6, 'LAEA tile_id must have 6 characters'
    assert tile_id[0] in ['W', 'E'], 'LAEA tile_id first character must be in W,E'
    assert tile_id[3] in ['S', 'N'], 'LAEA tile_id first character must be in S,N'
    assert set(tile_id[1:3] + tile_id[4:6]).issubset(
        set('0123456789')), 'LAEA tile_id second, third, fifth and sixth characters must be digits'
    return tile_id


def universal_l1c_id(product_id):
    product_id = os.path.basename(product_id) #make sure its just a name and not a path
    assert product_id.split('_')[1] == 'MSIL1C'
    product_id = product_id.replace('.SAFE','') #make sure the '.SAFE' is removed just like in amlthee indexes
    assert [len(el) for el in product_id.split('_')] == [3, 6, 15, 5, 4, 6,
                                                         15]  # check if syntax matches 'S2A_MSIL1C_20180102T102421_N0206_R065_T32TLR_20180102T123237' format
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


def datetime2str(datetime_obj):
    return datetime_obj.strftime('%Y%m%dT%H%M%SU%f')


def str2datetime(str_in):
    return datetime.strptime(str_in, '%Y%m%dT%H%M%SU%f')


def str2datetime_generic(str_in):
    if str_in is None:
        return None
    if isinstance(str_in, datetime):
        return str_in
    if not isinstance(str_in, str):
        raise Exception('expecting string in str2datetime_generic')
    if '+00:00' in str_in:
        return DateParser.parse(str_in.replace('+00:00', ''))
    return DateParser.parse(str_in)



def get_datetime_simple(dt_str):
    if isinstance(dt_str, datetime):
        return dt_str
    if dt_str is None:
        return None
    str_len = len(dt_str)
    if 'Z' in dt_str:
        if 22 <= str_len <= 27:
            return datetime.strptime(dt_str[0:-1] + '0' * (27 - str_len) + dt_str[-1], '%Y-%m-%dT%H:%M:%S.%fZ')
        elif str_len == 20:
            return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%SZ')
    else:
        if 21 <= str_len <= 26:
            return datetime.strptime(dt_str[0:-1] + '0' * (26 - str_len) + dt_str[-1], '%Y-%m-%dT%H:%M:%S.%f')
        elif str_len == 19:
            return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S')


def convert_dates_in_wekeo_dict(dico):
    for key in dico.keys():
        if key in ['startDate', 'completionDate', 'updated', 'published']:
            str_len = len(dico[key])
            date_out = get_datetime_simple(dico[key])
            if date_out is None:
                raise Exception('%s : invalid wekeo date format' % dico[key])
            else:
                dico[key] = date_out
    return dico


def get_and_del_param(params, key):
    '''
    Get, delete and return a parameter value from a dictionary.
    Return None if the parameter is missing.

    :param params: Parameter dictionary
    :param key: Parameter key
    :return: Parameter value
    '''
    if key not in params:
        raise CodedException('key %s missing' % key, exitcode=exitcodes.wrong_input_parameters)
    value = params[key]
    del params[key]
    return value




