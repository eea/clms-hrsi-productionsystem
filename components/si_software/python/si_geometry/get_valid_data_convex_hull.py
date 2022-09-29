#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" 
    geometry function to get convex hull of input data : for very large image, use_otb option can be activated to use OTB bandmath
"""

from si_geometry.geometry_functions import *
import rasterio
from shapely.ops import cascaded_union, polygonize
from shapely.geometry import Point, MultiLineString
from scipy.spatial import Delaunay

def alpha_shape(points, alpha):
    """
    Compute the alpha shape (concave hull) of a set
    of points.
    @param points: Iterable container of points.
    @param alpha: alpha value to influence the
        gooeyness of the border. Smaller numbers
        don't fall inward as much as larger numbers.
        Too large, and you lose everything!
    """
    if len(points) < 4:
        # When you have a triangle, there is no sense
        # in computing an alpha shape.
        return geometry.MultiPoint(list(points)).convex_hull

    coords = np.array([point.coords[0] for point in points])
    tri = Delaunay(coords)
    triangles = coords[tri.vertices]
    a = ((triangles[:,0,0] - triangles[:,1,0]) ** 2 + (triangles[:,0,1] - triangles[:,1,1]) ** 2) ** 0.5
    b = ((triangles[:,1,0] - triangles[:,2,0]) ** 2 + (triangles[:,1,1] - triangles[:,2,1]) ** 2) ** 0.5
    c = ((triangles[:,2,0] - triangles[:,0,0]) ** 2 + (triangles[:,2,1] - triangles[:,0,1]) ** 2) ** 0.5
    s = ( a + b + c ) / 2.0
    areas = (s*(s-a)*(s-b)*(s-c)) ** 0.5
    circums = a * b * c / (4.0 * areas)
    filtered = triangles[circums < (1.0 / alpha)]
    edge1 = filtered[:,(0,1)]
    edge2 = filtered[:,(1,2)]
    edge3 = filtered[:,(2,0)]
    edge_points = np.unique(np.concatenate((edge1,edge2,edge3)), axis = 0).tolist()
    m = MultiLineString(edge_points)
    triangles = list(polygonize(m))
    return cascaded_union(triangles), edge_points
    
    
def clean_exterior_shapes(list_of_shapes):
    list_out = []
    for poly in list_of_shapes:
        if isinstance(poly, Polygon):
            poly_loc = Polygon(poly.exterior)
            if not poly_loc.is_valid:
                poly_loc = poly_loc.buffer(0.)
            if isinstance(poly_loc, MultiPolygon):
                print('Polygon converted to multipolygon after buffering')
                list_out += clean_exterior_shapes(poly_loc)
                continue
            if len(poly_loc.exterior.coords) > 1000:
                print('Detected shape of len %d polygon '%len(poly_loc.exterior.coords))
                poly_loc = poly_loc.convex_hull
                print('  -> taking convex hull')
            list_out.append(poly_loc)
        elif isinstance(poly, MultiPolygon):
            list_out += clean_exterior_shapes(poly)
        else:
            raise Exception('type %s not accepted in clean_exterior_shapes, must be Polygon or MultiPolygon'%type(poly))
    return list_out
    
    
def points_from_shapes(list_of_shapes):
    list_out = []
    for poly in list_of_shapes:
        if isinstance(poly, Polygon):
            poly_loc = Polygon(poly.exterior)
            if not poly_loc.is_valid:
                poly_loc = poly_loc.buffer(0.)
            if isinstance(poly_loc, MultiPolygon):
                print('Polygon converted to multipolygon after buffering')
                list_out += points_from_shapes(poly_loc)
                continue
            # ~ if len(poly_loc.exterior.coords) > 1000:
                # ~ print('Detected shape of len %d polygon '%len(poly_loc.exterior.coords))
                # ~ poly_loc = poly_loc.convex_hull
                # ~ print('  -> taking convex hull')
            xloc, yloc = poly_loc.exterior.coords.xy
            list_out += [Point(xxloc, yyloc) for xxloc, yyloc in zip(xloc, yloc)]
        elif isinstance(poly, MultiPolygon):
            list_out += points_from_shapes(poly)
        else:
            raise Exception('type %s not accepted in clean_exterior_shapes, must be Polygon or MultiPolygon'%type(poly))
    return list_out
    
    
    
    
def get_valid_data_convex_hull(input_raster, valid_values=None, invalid_values=None, proj_out=None, temp_dir=None, use_otb=False, ram=None, npoints_per_edge=3, alpha=None):
    """extracts convex hull of valid data within input_raster.
    
    :param valid_values: list of values that must be considered valid within raster (optional)
    :param invalid_values: list of values that must be considered invalid within raster (optional)
    :param temp_dir: directory to use for temp file creation
    
    valid_values and invalid_values parameters cannot both be specified.
    if they are both not specified, the raster nodata value will be taken as an invalid value."""
    
    if temp_dir is None:
        temp_dir = os.getcwd()
    
    assert os.path.exists(input_raster) #check that input raster exists
    
    #if input values are not defined, read nodata value in raster
    if valid_values is not None:
        assert invalid_values is None, 'valid_values and invalid_values parameters cannot both be specified'
        valid_values = list_form(valid_values)
    else:
        if invalid_values is None:
            ds = gdal.Open(input_raster, gdal.GA_ReadOnly)
            nodata_value = ds.GetRasterBand(1).GetNoDataValue()
            ds = None
            del ds
            if nodata_value is None:
                invalid_values = []
            else:
                invalid_values = [nodata_value]
        invalid_values = list_form(invalid_values)
    
    #create temp dir and define file names
    temp_dir_loc = tempfile.mkdtemp(prefix='convex_hull_computing', dir=temp_dir)
    nodata_raster = os.path.join(temp_dir_loc, 'nodata_raster.tif')
    shp_file = os.path.join(temp_dir_loc, 'data_zone_selection.shp')

    #make a raster with 0 on invalid values and 1 elsewhere
    if use_otb:
        from si_geometry.geometry_functions_otb import band_math, otb
        if valid_values is not None:
            band_math([input_raster], nodata_raster, '(%s)?1:0'%(' or '.join(['(im1b1==%s)'%value for value in valid_values])), ram=ram, out_type=otb.ImagePixelType_uint8)
        elif len(invalid_values) > 0:
            band_math([input_raster], nodata_raster, '(%s)?0:1'%(' or '.join(['(im1b1==%s)'%value for value in invalid_values])), ram=ram, out_type=otb.ImagePixelType_uint8)
        else:
            band_math([input_raster], nodata_raster, '((A0==0)or(A0!=0))?1:0', ram=ram, out_type=otb.ImagePixelType_uint8)
    else:
        if valid_values is not None:
            bit_bandmath(nodata_raster, gdal.Info(input_raster, format='json'), [[{'sources': [{'filepath': input_raster, 'bandnumber': 1, 'unpack_bits': False}], \
                'operation': "logical_array_list_operation([%s], 'or')"%(','.join(['A0==%s'%val for val in valid_values]))}]], \
                compress=True, add_overviews=False, use_default_cosims_config=False)
        elif len(invalid_values) > 0:
            bit_bandmath(nodata_raster, gdal.Info(input_raster, format='json'), [[{'sources': [{'filepath': input_raster, 'bandnumber': 1, 'unpack_bits': False}], \
                'operation': "logical_array_list_operation([%s], 'or')"%(','.join(['A0==%s'%val for val in invalid_values]))}]], \
                compress=True, add_overviews=False, use_default_cosims_config=False)
        else:
            bit_bandmath(nodata_raster, gdal.Info(input_raster, format='json'), [[{'sources': [{'filepath': input_raster, 'bandnumber': 1, 'unpack_bits': False}], \
                'operation': "np.logical_or(A0==0, A0!=0)"}]], \
                compress=True, add_overviews=False, use_default_cosims_config=False)
        
    ds = gdal.Open(nodata_raster, gdal.GA_Update)
    nodata_value = ds.GetRasterBand(1).SetNoDataValue(0.)
    ds = None
    del ds
    
    #check if nodata_raster is with GCPS then gdalwarp to EPSG:4326, and replace file
    with rasterio.open(nodata_raster) as ds:
        is_gcps = hasattr(ds, 'gcps')
    if is_gcps:
        nodata_raster_reproj_loc = nodata_raster.replace('.tif', '_reproj.tif')
        subprocess.check_call(' '.join(['gdalwarp', '-t_srs', "'epsg:4326'", '-r', 'near', nodata_raster, nodata_raster_reproj_loc]), shell=True)
        os.unlink(nodata_raster)
        shutil.move(nodata_raster_reproj_loc, nodata_raster)
        proj_in = 'epsg:4326'
    
    #compute envelopping polygon in gml format (using gdal_polygonize and convexhull)
    subprocess.check_call(['gdal_polygonize.py', '-8', nodata_raster, shp_file])
    
    #compute convex hull and generate new gml file
    shapes = []
    with fiona.open(shp_file) as ds:
        if proj_in is None:
            proj_in = ds.meta['crs_wkt']
        for inv in list(ds.items()):
            shape_loc = shape(inv[1]['geometry'])
            if isinstance(shape_loc, Polygon) or isinstance(shape_loc, MultiPolygon):
                shapes.append(shape_loc)
    
    if alpha is None:
        print('Cleaning shapes')
        shapes = clean_exterior_shapes(shapes)
        print('Computing cascaded union')
        polygon_convex_hull = cascaded_union(shapes)
        polygon_convex_hull = polygon_convex_hull.convex_hull
    else:
        print('Computing points from shapes')
        points_loc = points_from_shapes(shapes)
        print('Computing alpha shape')
        polygon_convex_hull, _ = alpha_shape(points_loc, alpha)
        
    if not isinstance(polygon_convex_hull, Polygon):
        try:
            print(polygon_convex_hull.wkt)
        except:
            print('Could not print geometry, trying to print object')
            print(polygon_convex_hull)
        raise CodedException('Input product is full of NANs', exitcode=fsc_rlie_exitcodes.l1c_fullnan)

    
    #if a different projection is requested for output, reproject polygon
    if proj_out is not None:
        if proj_in != proj_out:
            polygon_convex_hull = Polygon(project_coords_to_different_coordinate_system(polygon_convex_hull.exterior.coords, proj_in, proj_out, npoints_per_edge=npoints_per_edge))
        
    #remove temp dir loc
    shutil.rmtree(temp_dir_loc)
    
    return polygon_convex_hull
    
    
#handle GCPS : does not work perfectly
    # ~ #in case of gcps, coordinate system is not passed to shapefile => apply geotransform manually
    # ~ if proj_in in [None, '']:
        # ~ with rasterio.open(nodata_raster) as ds:
            # ~ assert hasattr(ds, 'gcps')
            # ~ assert len(ds.gcps[0]) > 0
            # ~ proj_in = ds.gcps[1]
            # ~ xx, yy = polygon_convex_hull.exterior.coords.xy
            # ~ xx, yy = rasterio.transform.xy(rasterio.transform.from_gcps(ds.gcps[0]), yy, xx)
            # ~ polygon_convex_hull = Polygon(zip(xx, yy))


if __name__ == '__main__':
    
    import argparse
    parser = argparse.ArgumentParser(description='This script is used to get convex hull from tif file')
    parser.add_argument("input_tif", type=str, required=True, help='input tif path')
    parser.add_argument("--valid_values", type=str, help='valid values separated by coma')
    parser.add_argument("--invalid_values", type=str, help='invalid values separated by coma')
    parser.add_argument("--proj_out", type=str, default='EPSG:4326', help='output projection, default = EPSG:4326')
    parser.add_argument("--temp_dir", type=str, help='temp_dir')
    args = parser.parse_args()
    
    if args.valid_values is not None:
        if '.' in args.valid_values:
            args.valid_values = [float(el) for el in args.valid_values.replace(' ','').split(',')]
        else:
            args.valid_values = [int(el) for el in args.valid_values.replace(' ','').split(',')]
    if args.invalid_values is not None:
        if '.' in args.invalid_values:
            args.invalid_values = [float(el) for el in args.invalid_values.replace(' ','').split(',')]
        else:
            args.invalid_values = [int(el) for el in args.invalid_values.replace(' ','').split(',')]
    
    dico = get_valid_data_convex_hull(args.input_tif, valid_values=args.valid_values, invalid_values=args.invalid_values, proj_out=args.proj_out, temp_dir=temp_dir)
    print(dico)
    
