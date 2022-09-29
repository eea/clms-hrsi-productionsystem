#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
from datetime import datetime, timedelta

json_encode_methods = []
json_decode_methods = []

############# datetime
datetime_json_format = 'datetime:%Y-%m-%dT%H:%M:%S.%f'

#encode
def datetime_json_encode_method(value):
    if isinstance(value, datetime):
        return value.strftime(datetime_json_format), True
    return value, False
json_encode_methods.append(datetime_json_encode_method)

#decode
def datetime_json_decode_method(value):
    if isinstance(value, str):
        match = re.search(r'datetime:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}', value)
        if match:
            try:
                value_new = datetime.strptime(value, datetime_json_format)
                return value_new, True
            except:
                pass
    return value, False
json_decode_methods.append(datetime_json_decode_method)
#############

class JsonTunedEncoder(json.JSONEncoder):
    
    def default(self, obj):
        for method in json_encode_methods:
            value, encoded = method(obj)
            if encoded:
                return value
        return json.JSONEncoder.default(obj)
 
 

class JsonTunedDecoder(json.JSONDecoder):
    
    def __init__(self, **kwargs):
        super().__init__(object_hook=self.post_decode, **kwargs)

    def post_decode(self, dico):
        for key, value in dico.items():
            for method in json_decode_methods:
                value_new, decoded = method(value)
                if decoded: #exit when first match occurs
                    dico[key] = value_new
                    break
        return dico
 
 
def dump_json(dico, filepath=None, indent=None):
    if filepath is not None:
        with open(filepath, mode='w') as ds:
            json.dump(dico, ds, indent=indent, cls=JsonTunedEncoder)
    else:
        return json.dumps(dico, indent=indent, cls=JsonTunedEncoder)
        
        
def load_json(str_in):
    if os.path.isfile(str_in):
        with open(str_in) as ds:
            return json.load(ds, cls=JsonTunedDecoder)
    else:
        return json.loads(str_in, cls=JsonTunedDecoder)
        
        
        
if __name__ == '__main__':
    
    dico_in = {'ok': datetime(2019,1,1), 1: 0.1, 2: 'echo'}
    str_out = dump_json(dico_in)
    dico_out = load_json(str_out)
    print(str_out)
    print(dico_out)
    
