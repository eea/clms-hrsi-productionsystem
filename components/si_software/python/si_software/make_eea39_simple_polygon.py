#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" 
    geometry functions
"""

from si_common.common_functions import *


from shapely.geometry import mapping, shape, MultiPolygon, Polygon, box
from shapely.ops import cascaded_union
from pyproj import Transformer, CRS
import fiona


    

    
def project_polygon_exterior_to_different_coordinate_system(polygonbox_in, proj_in_str, proj_out_str, npoints_per_edge=100):
    """projects a polygon exterior to a bounding box in a different coordinate system.
    Since edges don't stay straight across changes in coordinate systems, we add npoints_per_edge points across the edges before projection.
    """

    coords_in = list(polygonbox_in.exterior.coords)
    dl = 1./npoints_per_edge
    coords_out = []
    proj_in = CRS.from_user_input(proj_in_str)
    proj_out = CRS.from_user_input(proj_out_str)
    transformer = Transformer.from_crs(proj_in, proj_out, always_xy=True)
    for icoor in range(len(coords_in)-1):
        x_proj, y_proj = transformer.transform([coords_in[icoor][0]+(coords_in[icoor+1][0]-coords_in[icoor][0])*ii*dl for ii in range(npoints_per_edge)], \
            [coords_in[icoor][1]+(coords_in[icoor+1][1]-coords_in[icoor][1])*ii*dl for ii in range(npoints_per_edge)])
        coords_out += list(zip(x_proj, y_proj))
    return Polygon(coords_out)
    


    
    
def get_shape_convex_hull(input_shapefile, proj_out=None, shapefile_out=None):
    #compute convex hull
    shapes = []
    with fiona.open(input_shapefile) as ds:
        proj_in = ds.meta['crs_wkt']
        for inv in list(ds.items()):
            shapes.append(shape(inv[1]['geometry']))
    polygon_convex_hull = cascaded_union(shapes).convex_hull
    if proj_out is not None:
        polygon_convex_hull = project_polygon_exterior_to_different_coordinate_system(polygon_convex_hull, proj_in, proj_out, npoints_per_edge=3)
    if shapefile_out is not None:
        with fiona.open(shapefile_out, 'w', 'ESRI Shapefile', {'geometry': 'Polygon', 'properties': {}}) as ds:
            ds.write({'geometry': mapping(polygon_convex_hull), 'properties': {}})
        with open(shapefile_out.replace('.shp', '.prj'), mode='w') as ds:
            ds.write(CRS.from_user_input(proj_in).to_wkt())
    return polygon_convex_hull
    
    
def get_shape_wkt(input_shapefile):
    shapes = []
    with fiona.open(input_shapefile) as ds:
        proj_in = ds.meta['crs_wkt']
        for inv in list(ds.items()):
            shapes.append(shape(inv[1]['geometry']))
    assert len(shapes) == 1
    return shapes[0].wkt


    
    

if __name__ == '__main__':
    
    # ~ polygon_convex_hull = get_shape_convex_hull('/mnt/data_hdd/snow_and_ice/AOI/si_software_data/AOI_EEA39/s2tiles_eea39/s2tiles_eea39.shp', shapefile_out='test/test.shp')
    # ~ print(polygon_convex_hull.wkt)

    wkt = get_shape_wkt('test/eea39_simplified_shape.shp')
    print(wkt)
