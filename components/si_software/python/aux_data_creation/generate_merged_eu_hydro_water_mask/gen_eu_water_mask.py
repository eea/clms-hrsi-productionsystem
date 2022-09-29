#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os, sys, shutil
from shapely.geometry import mapping, shape
import fiona
import json
from shapely.ops import cascaded_union, unary_union
from pyproj import CRS

import si_utils.crop_shapefiles
from aux_data_creation.aux_data_list_files import aux_data_list_files

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

    

if __name__ == '__main__':
    
    laea_prj = 'PROJCS["ETRS89_LAEA_Europe",GEOGCS["GCS_ETRS_1989",DATUM["D_ETRS_1989",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Lambert_Azimuthal_Equal_Area"],PARAMETER["latitude_of_origin",52],PARAMETER["central_meridian",10],PARAMETER["false_easting",4321000],PARAMETER["false_northing",3210000],UNIT["Meter",1]]'
    
    import argparse
    parser = argparse.ArgumentParser(description="create merged EU-HYDRO shapefiles", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("eu_hydro_basin_geodatabases_path", type=str, help="eu_hydro_basin_geodatabases_path")
    parser.add_argument("output_dir", type=str, help="directory to store merged EU hydro shapefiles")
    args = parser.parse_args()
    
    fol_src = args.eu_hydro_basin_geodatabases_path
    fol_out = args.output_dir

    gdb_files = [os.path.join(fol_src, el) for el in os.listdir(fol_src) if '.gdb' in el]
    get_polygons_from_gdb(gdb_files, os.path.join(fol_out, 'eu_hydro_riverbasins.shp'), \
        layer_selection=['RiverBasins'], prj=laea_prj, save_basin_name_as_property=True)
    # ~ get_polygons_from_gdb(gdb_files, os.path.join(fol_out, 'eu_hydro_landwater.shp'), \
        # ~ layer_selection=['InlandWater', 'River_Net_p', 'Canals_p', 'Ditches_p', 'Transit_p'], prj=laea_prj)
    # ~ get_polygons_from_gdb(gdb_files, os.path.join(fol_out, 'eu_hydro_landwater_with_coasts.shp'), \
        # ~ layer_selection=['InlandWater', 'River_Net_p', 'Canals_p', 'Ditches_p', 'Transit_p', 'Coastal_p'], prj=laea_prj)
    

