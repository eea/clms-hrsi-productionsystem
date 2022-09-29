#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" 
    Author: rjugier
    This module is about "tuning" yaml I/O
"""

import re
from yaml import load, add_implicit_resolver, add_constructor, Loader, dump, Dumper

from .exitcodes import *

pattern = re.compile( r'^(.*)\$\{(.*)\}(.*)$' )



def pathex_constructor(loader,node):
    precedingPath = loader.construct_scalar(node)
    output_strings = []
    while ('${' in precedingPath) and ('}' in precedingPath):
        precedingPath, envVar, remainingPath = pattern.match(precedingPath).groups()
        output_strings.append(remainingPath)
        output_strings.append(os.environ[envVar])
    output_strings.append(precedingPath)
    return ''.join(output_strings[::-1])


add_implicit_resolver("!pathex", pattern)
add_constructor('!pathex', pathex_constructor)


def load_yaml(filename_or_string_input, env_vars=False, complicated_input=False, string_input=False):
    
    if string_input:
        input_string = filename_or_string_input
    else:
        with open(filename_or_string_input) as descr:
            input_string = descr.read()
    if env_vars or complicated_input:
        dico = load(input_string, Loader=Loader)
    else:
        dico = load(input_string, Loader=Loader)

    if isinstance(dico, dict):
        if 'include' in dico:
            if isinstance(dico['include'], list):
                for el in dico['include']:
                    if os.path.exists(el):
                        dico.update(load_yaml(el, env_vars=env_vars, complicated_input=complicated_input))
                    else:
                        raise MainArgError('file %s does not exist'%el)
            elif isinstance(dico['include'], dict):
                for el in dico['include']:
                    if os.path.exists(dico['include'][el]):
                        dico.update({el: load_yaml(dico['include'][el], env_vars=env_vars, complicated_input=complicated_input)})
                    else:
                        raise MainArgError('file %s does not exist'%dico['include'][el])
            else:
                if os.path.exists(dico['include']):
                    dico.update(load_yaml(dico['include'], env_vars=env_vars, complicated_input=complicated_input))
                else:
                    raise MainArgError('file %s does not exist'%dico['include'])
    return dico


def dump_yaml(yaml_dict, filename=None):
    if filename is not None:
        with open(filename, mode='w') as ds:
            dump(yaml_dict, ds, default_flow_style=False, indent=2)
    else:
        return dump(yaml_dict, default_flow_style=False, indent=2)
        

                    
