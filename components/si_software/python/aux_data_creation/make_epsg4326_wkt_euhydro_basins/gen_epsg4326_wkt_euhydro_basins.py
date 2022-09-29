#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil
from si_geometry.geometry_functions import *

assert tuple([int(el) for el in fiona.__version__.split('.')]) >= (1,8,11), 'fiona versions before 1.8.11 have been known to exit with error when handling some EU-HYDRO geometries'

    
    
def write_multipolygons(multipolygons, shapefile_out, properties=None):
    
    with fiona.open(shapefile_out, 'w', 'ESRI Shapefile', {'geometry': 'MultiPolygon', 'properties': properties['metadata']}) as ds:
        for ii, multipolygon in enumerate(multipolygons):
            if multipolygon.geom_type == 'Polygon':
                ds.write({'geometry': mapping(MultiPolygon([multipolygon])), 'properties': properties['data'][ii]})
            elif multipolygon.geom_type == 'MultiPolygon':
                ds.write({'geometry': mapping(multipolygon), 'properties': properties['data'][ii]})
            else:
                raise Exception('Failure: File %s not written ; geometry %s unknown'%(shapefile_out, multipolygon.geom_type))
        
        

def get_polygons_from_gdb(gdb_path_list, shapefile_out, layer_selection=None, prj=None, polygon_union=None, save_basin_name_as_property=False):
    
    assert shapefile_out.split('.')[-1] == 'shp'
    
    os.makedirs(os.path.dirname(shapefile_out), exist_ok=True)
    
    if save_basin_name_as_property:
        properties = {'metadata': {'name': 'str'}, 'data': []}
    multipolygons = []
    for gdb_path in gdb_path_list:
        print('Processing %s...'%gdb_path)
        layers_loc = list(fiona.listlayers(gdb_path))
        print(' -> layers found : %s'%(', '.join(layers_loc)))
        if layer_selection is not None:
            layers_loc = list(set(layers_loc) & set(layer_selection))
        for layer_name in layers_loc:
            print('  -> parsing layer %s'%layer_name)
            with fiona.open(gdb_path, layer=layer_name) as ds:
                features_loc = []
                for feature in ds:
                    loc_shape = shape(feature['geometry'])
                    if loc_shape.type in ['Polygon', 'MultiPolygon']:
                        features_loc.append(loc_shape)
                    else:
                        print('  -> warning: encountered non polygon geometry %s'%loc_shape.type)
                if save_basin_name_as_property:
                    if len(features_loc) == 0:
                        raise Exception('at least 1 feature expected')
                    elif len(features_loc) > 1:
                        multipolygons += [unary_union(features_loc)]
                    else:
                        multipolygons += features_loc
                    properties['data'].append({'name': os.path.basename(gdb_path).split('.')[0].lower()})
                else:
                    multipolygons += features_loc
    
    if len(multipolygons) == 0:
        print('Not polygons loaded, not writing %s ...'%shapefile_out)
        return
    
    if polygon_union is not None:
        if save_basin_name_as_property:
            raise Exception('polygon_union not None and save_basin_name_as_property True are incompatible options')
        multipolygons = [polygon_union(multipolygons)]
    
    print('Writing %s ...'%shapefile_out)
    write_multipolygons(multipolygons, shapefile_out, properties=properties)
    if prj is not None:
        with open(shapefile_out.replace('.shp', '.prj'), mode='w') as ds:
            ds.write(CRS.from_user_input(prj).to_wkt())



def gen_epsg4326_wkt_euhydro_basins(fol_gdb, output_json):
    
    gdb_files = [os.path.join(fol_gdb, el) for el in os.listdir(fol_gdb) if '.gdb' in el]
    get_polygons_from_gdb(gdb_files, os.path.join(fol_out, 'eu_hydro_riverbasins.shp'), layer_selection=['RiverBasins'], prj=laea_prj, save_basin_name_as_property=True)
    

if __name__ == '__main__':
    

    import argparse
    parser = argparse.ArgumentParser(description="gen_epsg4326_wkt_euhydro_basins", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--eu_hydro_basin_geodatabases_path", type=str, help="input shapefile containing euhydro river basins")
    parser.add_argument("--output_json", type=str, help="output json")
    args = parser.parse_args()
    
    gen_epsg4326_wkt_euhydro_basins(args.eu_hydro_basin_geodatabases_path, args.output_json)
    

