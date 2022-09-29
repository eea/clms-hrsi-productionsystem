#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, shutil, subprocess
import fiona
# ~ from shapely.geometry import mapping, shape, MultiPolygon, Polygon, box
from pyproj import CRS

def convert_part1_s2shp_to_part2(part1_shp, part2_shp):
    
    with fiona.open(part1_shp) as ds_in, fiona.open(part2_shp, 'w', 'ESRI Shapefile', {'geometry': 'Polygon', 'properties': {'Name': 'str'}}) as ds_out: 
        for inv in list(ds_in.items()):
            ds_out.write({'geometry': inv[1]['geometry'], 'properties': {'Name': inv[1]['properties']['tile_name']}})
    with open(part2_shp.replace('.shp', '.prj'), mode='w') as ds:
        ds.write('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]')



if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='convert part1 shapefile into part 2 shapefile')
    parser.add_argument("part1_shp", type=str, help='part1 shapefile path')
    parser.add_argument("part2_shp", type=str, help='part2 shapefile path (output)')
    args = parser.parse_args()

    convert_part1_s2shp_to_part2(args.part1_shp, args.part2_shp)
