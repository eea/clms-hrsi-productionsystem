#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" 
    geometry functions
"""

from si_common.common_functions import *

try:
    from osgeo import gdal
except:
    import gdal
from shapely.geometry import mapping, shape, MultiPolygon, Polygon, box
from shapely.ops import cascaded_union, unary_union
import shapely.wkt
from pyproj import Transformer, CRS
import shapely.affinity
from descartes import PolygonPatch
import fiona

from si_utils.compress_geotiff import compress_geotiff_file, replicate_directory_with_compressed_tiffs

from si_geometry.combine_bits_geotiff import bit_bandmath



def polygonshape_to_wkt(src_shape):
    if src_shape is None:
        return None
    if isinstance(src_shape, str):
        if '.shp' in src_shape:
            assert os.path.exists(src_shape), 'since src_shape string ended with .shp, we assumed it was a path to a shapefile but it does not appear so : %s'%src_shape
            with fiona.open(src_shape) as ds:
                footprint_wkt = [shape(inv[1]['geometry']) for inv in list(ds.items())][0]
        else:
            if '.wkt' in src_shape:
                assert os.path.exists(src_shape), 'since src_shape string ended with .wkt, we assumed it was a path to a WKT file but it does not appear so : %s'%src_shape
                with open(src_shape) as ds:
                    footprint_wkt = ds.read().replace('\n','')
            elif 'POLYGON (('  not in src_shape:
                raise Exception('%s : format unknown to deduce WKT shape'%src_shape)
            try:
                footprint_wkt = shapely.wkt.loads(src_shape)
            except:
                print(src_shape)
                print('src_shape could not be read as a proper WKT string : ERROR !')
                raise
    else:
        footprint_wkt = src_shape
    assert isinstance(footprint_wkt, Polygon)
    return footprint_wkt.wkt






def quick_geom_conversion_hack(geom_in):
    if isinstance(geom_in, str):
        return shapely.wkt.loads(geom_in)
    elif isinstance(geom_in, dict):
        return shape(geom_in)
    return geom_in

def get_s1_intersection_with_s2(s1geom, s2geom_dict):
    s1geom_loc = quick_geom_conversion_hack(s1geom)
    s2tiles_intersect_list = []
    for tile_id, geometry_s2 in s2geom_dict.items():
        geometry_s2_loc = quick_geom_conversion_hack(geometry_s2)
        if s1geom_loc.intersects(geometry_s2_loc):
            s2tiles_intersect_list.append(tile_id)
    return sorted(s2tiles_intersect_list)


def getWKT_PRJ(epsg_code):
    wkt = requests.get("http://spatialreference.org/ref/epsg/{0}/prettywkt/".format(epsg_code))
    return wkt.text.replace(" ","").replace("\n", "")

def proj_to_prj(proj_in):
    # ~ if proj_in.lower() == 'epsg:4326':
        # ~ return 'GEOGCS["WGS84",DATUM["WGS_1984",SPHEROID["WGS84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
    return getWKT_PRJ(CRS.from_user_input(proj_in).to_epsg())





def read_band(input_raster, band_no=1):
    ds = gdal.Open(input_raster)
    band = ds.GetRasterBand(band_no)
    ar_loc = band.ReadAsArray()
    ds, band = None, None
    del ds, band
    return ar_loc

def write_band(input_raster, data_ar, band_no=1):
    ds = gdal.Open(input_raster, 1)
    band = ds.GetRasterBand(band_no)
    band.WriteArray(data_ar)
    ds, band = None, None
    del ds, band



def set_gdal_otb_itk_env_vars(nprocs=None, max_ram=None):
    """max_ram must be in MB"""
    if nprocs is not None:
        assert isinstance(nprocs, int), 'nprocs must an integer'
        assert nprocs <= cpu_count(), 'nprocs must be <= number of procs available'
        os.environ["GDAL_NUM_THREADS"] = '%d'%nprocs
        os.environ["OPJ_NUM_THREADS"] = '%d'%nprocs
        os.environ["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = '%d'%nprocs
    if max_ram is not None:
        assert isinstance(max_ram, int), 'max_ram must an integer'
        os.environ["OTB_MAX_RAM_HINT"] = '%d'%max_ram
        # ~ coded_assert(max_ram < 100000, 'max_ram must be < 100000 to avoid confusion for GDAL_CACHEMAX', exitcodes.wrong_input_parameters)
        # ~ os.environ["GDAL_CACHEMAX"] = '%d'%max_ram
        
        
        


def apply_dem_mask(input_raster, dem_file, compress=False):
    
    ds = gdal.Open(dem_file)
    dem_data = np.ma.masked_invalid(ds.GetRasterBand(1).ReadAsArray())
    no_data_value = ds.GetRasterBand(1).GetNoDataValue()
    dem_data.mask[dem_data==no_data_value] = True
    ds = None
    del ds
    
    ds = gdal.Open(input_raster, 1)
    band = ds.GetRasterBand(1)
    no_data_value = band.GetNoDataValue()
    input_data = band.ReadAsArray()
    input_data[dem_data.mask] = no_data_value
    band.WriteArray(input_data)
    ds = None
    del ds
    
    if compress:
        compress_geotiff_file(input_raster)
    


def initialize_raster(gdal_info_dict, output_file, nbands=1, dtype='u1', nodata_value=None, fill_all_value=None, random_dict=None, compress=False):
    
    assert (fill_all_value is None) or (random_dict is None)
    
    if dtype in ['u1', np.uint8, gdal.GDT_Byte]:
        gdal_type = gdal.GDT_Byte
    elif dtype in ['u2', np.uint16, gdal.GDT_UInt16]:
        gdal_type = gdal.GDT_UInt16
    elif dtype in ['u4', np.uint32, gdal.GDT_UInt32]:
        gdal_type = gdal.GDT_UInt32
    elif dtype in ['i2', np.int16, gdal.GDT_Int16]:
        gdal_type = gdal.GDT_Int16
    elif dtype in ['i4', np.int32, gdal.GDT_Int32]:
        gdal_type = gdal.GDT_Int32
    elif dtype in ['f4', np.float32, gdal.GDT_Float32]:
        gdal_type = gdal.GDT_Float32
    elif dtype in ['f8', np.float64, gdal.GDT_Float64]:
        gdal_type = gdal.GDT_Float64

    target_ds = gdal.GetDriverByName('GTiff').Create(output_file, gdal_info_dict['size'][0], gdal_info_dict['size'][1], nbands, gdal_type)
    target_ds.SetGeoTransform(tuple(gdal_info_dict['geoTransform']))
    for iband in range(nbands):
        band = target_ds.GetRasterBand(iband+1)
        if nodata_value is not None:
            band.SetNoDataValue(float(nodata_value))
        array_in = band.ReadAsArray()
        shape_in = np.shape(array_in)
        if random_dict is None: #fill with zeros if no value fill guidelines are given
            if fill_all_value is None:
                array_in[:,:] = np.zeros(shape_in, dtype=array_in.dtype)
            else:
                array_in[:,:] = np.ones(shape_in, dtype=array_in.dtype)*fill_all_value
        else:
            if 'weights' is None:
                random_dict['weights'] = None
            array_in[:,:] = np.random.choice(random_dict['values'], size=shape_in, p=random_dict['weights']).astype(array_in.dtype)
        band.WriteArray(array_in)
    target_ds.SetProjection(gdal_info_dict['coordinateSystem']['wkt'])
    band.FlushCache()
    target_ds = None
    del target_ds
    if compress:
        compress_geotiff_file(output_file)
    return array_in



def reproject(source_file_or_ds, gdal_info_dict_dst, target_file, xRes=20., yRes=20., srcNodata=255, dstNodata=255, resampleAlg='near', return_array=False, \
    remove_target_file=False, gdalwarp_use_shell=None):

    #get coordinate system in proj4 format
    proj = CRS.from_user_input(gdal_info_dict_dst['coordinateSystem']['wkt']).to_proj4()
    
    if gdalwarp_use_shell is None:
        if 'gdalwarp_use_shell' in os.environ:
            gdalwarp_use_shell = os.environ['gdalwarp_use_shell'].lower() in ['1', 'true', 't']
        else:
            gdalwarp_use_shell = False
        
    
    if not gdalwarp_use_shell:
        gdal.Warp(target_file, source_file_or_ds, options=gdal.WarpOptions(format='GTiff', \
            outputBounds=tuple(gdal_info_dict_dst['cornerCoordinates']['lowerLeft'] + gdal_info_dict_dst['cornerCoordinates']['upperRight']), \
            xRes=xRes, yRes=yRes, dstSRS=proj, outputType=gdal.GDT_Byte, \
            warpMemoryLimit=4000., \
            resampleAlg=resampleAlg, \
            creationOptions=['compress=deflate', 'zlevel=4'], \
            srcNodata=srcNodata, dstNodata=dstNodata))
    else:
        corners = str(gdal_info_dict_dst['cornerCoordinates']['lowerLeft'][0]) + ' ' + str(gdal_info_dict_dst['cornerCoordinates']['lowerLeft'][1]) + ' ' + \
            str(gdal_info_dict_dst['cornerCoordinates']['upperRight'][0]) + ' ' + str(gdal_info_dict_dst['cornerCoordinates']['upperRight'][1])
        gdal_warp_cmd = ['gdalwarp', '-overwrite', '-r', resampleAlg, '-ot', 'Byte', '-of', 'GTIFF', '-tr', str(xRes), str(yRes), '-te', corners, '-t_srs', '"%s"'%proj, \
            '-srcnodata', str(srcNodata), '-dstnodata', str(dstNodata), '-co', '"compress=deflate"', '-co', '"zlevel=4"', source_file_or_ds, target_file]
        print(' '.join(gdal_warp_cmd))
        subprocess.check_call(' '.join(gdal_warp_cmd), shell=True)

    if return_array:
        ar_loc = read_band(target_file)
        if remove_target_file:
            os.unlink(target_file)
        return ar_loc
        
        
        
        

def get_minimum_container_info(dico_container, dico_containee):
    dico_out = dict()
    return dico_out
    
    
    
    
def gdal_info_rescale(gdal_info_dict, xRes=20., yRes=20.):
    gdal_info_dict_rescaled = copy.deepcopy(gdal_info_dict)
    gdal_info_dict_rescaled['size'] = [int(np.floor(np.abs(gdal_info_dict['size'][0]*gdal_info_dict['geoTransform'][1]/xRes))), \
        int(np.floor(np.abs(gdal_info_dict['size'][1]*gdal_info_dict['geoTransform'][5]/yRes)))]
    gdal_info_dict_rescaled['geoTransform'][1] = xRes
    gdal_info_dict_rescaled['geoTransform'][5] = -yRes
    return gdal_info_dict_rescaled
    

def project_coords_to_different_coordinate_system(coords_in, proj_in_str, proj_out_str, npoints_per_edge=100):
    """projects a polygon exterior to a bounding box in a different coordinate system.
    Since edges don't stay straight across changes in coordinate systems, we add npoints_per_edge points across the edges before projection.
    """
    dl = 1./npoints_per_edge
    coords_out = []
    proj_in = CRS.from_user_input(proj_in_str)
    proj_out = CRS.from_user_input(proj_out_str)
    transformer = Transformer.from_crs(proj_in, proj_out, always_xy=True)
    for icoor in range(len(coords_in)-1):
        x_proj, y_proj = transformer.transform([coords_in[icoor][0]+(coords_in[icoor+1][0]-coords_in[icoor][0])*ii*dl for ii in range(npoints_per_edge)], \
            [coords_in[icoor][1]+(coords_in[icoor+1][1]-coords_in[icoor][1])*ii*dl for ii in range(npoints_per_edge)])
        coords_out += list(zip(x_proj, y_proj))
    return coords_out
    
    
def project_polygon_to_different_coordinate_system(polygon_in, proj_in_str, proj_out_str, npoints_per_edge=100):
    """accepts polygons and multipolygons
    """
    
    if polygon_in.type == 'MultiPolygon':
        return MultiPolygon([project_polygon_to_different_coordinate_system(el, proj_in_str, proj_out_str, npoints_per_edge=npoints_per_edge) for el in polygon_in])
        
    assert polygon_in.type == 'Polygon', 'unhandled geometry : %s'%polygon_in.type
    return Polygon(project_coords_to_different_coordinate_system(list(polygon_in.exterior.coords), proj_in_str, proj_out_str, npoints_per_edge=npoints_per_edge), \
        [project_coords_to_different_coordinate_system(list(el.coords), proj_in_str, proj_out_str, npoints_per_edge=npoints_per_edge) for el in polygon_in.interiors])



def project_polygon_shapefile_to_different_coordinate_system(shapefile_in, shapefile_out, proj_out_str, proj_in_str=None, npoints_per_edge=10):
    
    multipolygons = []
    properties = []
    with fiona.open(shapefile_in) as ds:
        metadata = ds.meta
        if proj_in_str is None:
            proj_in_str = metadata['crs_wkt']
            if len(proj_in_str.replace(' ','').replace('\n','')) == 0:
                raise Exception('No shapefile .proj file: %s\n => coordinate system could not be retrieved from shapefile'%shapefile_path.replace('.shp', '.proj'))
        for inv in list(ds.items()):
            properties.append(inv[1]['properties'])
            multipolygons.append(shape(inv[1]['geometry']))
    
    with fiona.open(shapefile_out, 'w', 'ESRI Shapefile', {'geometry': 'MultiPolygon', 'properties': metadata['schema']['properties']}) as ds:
        for ii, multipolygon in enumerate(multipolygons):
            if isinstance(multipolygon, MultiPolygon) or isinstance(multipolygon, Polygon):
                multipolygon_loc = project_polygon_to_different_coordinate_system(multipolygon, proj_in_str, proj_out_str, npoints_per_edge=npoints_per_edge)
                if isinstance(multipolygon_loc, Polygon):
                    multipolygon_loc = MultiPolygon([multipolygon_loc])
                ds.write({'geometry': mapping(multipolygon_loc), 'properties': properties[ii]})
    with open(shapefile_out.replace('.shp', '.prj'), mode='w') as ds:
        ds.write(proj_to_prj(proj_out_str))
    
        
                
    
                
    
class RasterPerimeter(object):
    """ This object reads bounds and coordinate system from a raster file and is able to produce a bounding box for any coordinate system"""
    
    def __init__(self, raster_file_or_info):
        """ reads bounds and coordinate system from a raster file"""
        if isinstance(raster_file_or_info, dict):
            self.raster_file = None
            self.info = raster_file_or_info
        else:
            self.raster_file = raster_file_or_info
            self.info = gdal.Info(self.raster_file, format='json')
        coors = self.info['cornerCoordinates']
        self.proj = self.info['coordinateSystem']['wkt']
        self.perimeter_box = Polygon([(coors[el][0], coors[el][1]) for el in ['lowerLeft', 'lowerRight', 'upperRight', 'upperLeft']])
        
    def projected_perimeter(self, proj_in, npoints_per_edge=100):
        """adds a bounding box for a new coordinate system"""
        return Polygon(project_coords_to_different_coordinate_system(self.perimeter_box.exterior.coords, self.proj, proj_in, npoints_per_edge=npoints_per_edge))
        
        
    def to_epsg4326_shapefile(self, shapefile_out, npoints_per_edge=100):
        assert os.path.basename(shapefile_out).split('.')[-1] == 'shp', 'output shapefile filename must end by .shp'
        poly_4326 = self.projected_perimeter('epsg:4326', npoints_per_edge=npoints_per_edge)
        with fiona.open(shapefile_out, 'w', 'ESRI Shapefile', {'geometry': 'Polygon', 'properties': dict()}) as ds:
            ds.write({'geometry': mapping(poly_4326), 'properties': {}})
        with open(shapefile_out.replace('.shp', '.prj'), mode='w') as ds:
            ds.write(proj_to_prj('epsg:4326'))
    
    def get_lonlat_minmax(self):
        dico = dict()
        dico['latmin'], dico['lonmin'], dico['latmax'], dico['lonmax'] = self.projected_perimeter('epsg:4326').bounds
        return dico
        
    def intersects(self, raster_perim_obj):
        """checks if this raster intersects with another raster in a different (or same) coordinate system"""
        return self.projected_perimeter(raster_perim_obj.proj).intersects(raster_perim_obj.perimeter_box)
        

    def clip_multipolygon_shapefile_to_raster_perimeter(self, shapefile_path, shapefile_out, proj_in=None, verbose=1, outside=False):
        """clips (crops, geographical selection) a shapefile in any coordinate system to this raster's bounding box"""
        multipolygons = []
        properties = []
        with fiona.open(shapefile_path) as ds:
            
            metadata = ds.meta
            if proj_in is None:
                proj_in = metadata['crs_wkt']
                if len(proj_in.replace(' ','').replace('\n','')) == 0:
                    raise MainInputFileError('No shapefile .proj file: %s\n => coordinate system could not be retrieved from shapefile'%shapefile_path.replace('.shp', '.proj'))
            
            for inv in list(ds.items()):
                properties.append(inv[1]['properties'])
                multipolygons.append(shape(inv[1]['geometry']))
                
        #adds a perimeter for the proj_in coordinate system
        projected_perimeter = self.projected_perimeter(proj_in)
        
        #compute cropped shape
        multipolygon_out = []
        for ii, multipolygon in enumerate(multipolygons):
            if not projected_perimeter.intersects(box(*multipolygon.bounds)):
                if verbose > 0:
                    print('skipped %d/%d: no intersection with bounding box'%(ii+1, len(multipolygons)))
                continue
            try:
                geom_loc = multipolygon.intersection(projected_perimeter)
            except:
                continue
            if geom_loc.geom_type in ['MultiPolygon', 'Polygon']:
                multipolygon_out.append(geom_loc)
            else:
                multipolygon_out += [elem for elem in geom_loc if elem.geom_type in ['MultiPolygon', 'Polygon']]
            if verbose > 0:
                print('added: %d/%d'%(ii+1, len(multipolygons)))
        if len(multipolygon_out) == 0:
            if outside:
                multipolygon_out = projected_perimeter
            else:
                return False
        else:
            try:
                multipolygon_out = unary_union(multipolygon_out)
            except:
                multipolygon_out = unary_union([pol.buffer(0.) for pol in multipolygon_out])
            if outside:
                multipolygon_out = projected_perimeter.difference(multipolygon_out)
                if multipolygon_out.geom_type == 'GeometryCollection':
                    multipolygon_out = [el for el in multipolygon_out if el.geom_type in ['MultiPolygon', 'Polygon']]
                    if multipolygon_out:
                        try:
                            multipolygon_out = unary_union(multipolygon_out)
                        except:
                            multipolygon_out = unary_union([pol.buffer(0.) for pol in multipolygon_out])
                    else:
                        return False
        if multipolygon_out.geom_type == 'Polygon':
            multipolygon_out = MultiPolygon([multipolygon_out])
        if multipolygon_out.geom_type != 'MultiPolygon':
            raise RuntimeArgError('input type not supported : %s'%multipolygon_out.geom_type)
            
        
        with fiona.open(shapefile_out, 'w', 'ESRI Shapefile', {'geometry': 'MultiPolygon', 'properties': metadata['schema']['properties']}) as ds:
            ds.write({'geometry': mapping(multipolygon_out), 'properties': properties[ii]})
        with open(shapefile_out.replace('.shp', '.prj'), mode='w') as ds:
            ds.write(CRS.from_user_input(proj_in).to_wkt())
            
        return True
        





def examples():

    #1: test if 2 rasters from different coordinate systems intersect
    s2_tile_in = '/mnt/data_hdd/snow_and_ice/snow_and_ice_scheduler_full/test_bundle/l1c/S2A_MSIL1C_20180614T103021_N0206_R108_T32TLR_20180614T124154.SAFE/GRANULE/L1C_T32TLR_A015549_20180614T103021/IMG_DATA/T32TLR_20180614T103021_B01.jp2'
    eudem_in = '/mnt/data_hdd_temp/snow_and_ice/EU_DTM/eu_dem_unziped_1.1/eu_dem_v11_E40N20.TIF'
    s2_rasterperim = RasterPerimeter(s2_tile_in)
    print(s2_rasterperim.intersects(RasterPerimeter(eudem_in)))
    
    #2: test cropping shapefile
    s2_tile_in = '/mnt/data_hdd/snow_and_ice/snow_and_ice_scheduler_full/test_bundle/l1c/S2A_MSIL1C_20180614T103021_N0206_R108_T32TLR_20180614T124154.SAFE/GRANULE/L1C_T32TLR_A015549_20180614T103021/IMG_DATA/T32TLR_20180614T103021_B01.jp2'
    shapefile_in = '/mnt/data_hdd_temp/snow_and_ice/EU_DTM/eu_hydro_rivers_commonshape/eu_hydro_rivers_commonshape_merge/eu_hydro_rivers_commonshape.shp'
    shapefile_out = 'eu_hydro_T32TLR.shp'
    s2_rasterperim = RasterPerimeter(s2_tile_in)
    s2_rasterperim.clip_multipolygon_shapefile_to_raster_perimeter(shapefile_in, shapefile_out)


if __name__ == '__main__':
    
    
    examples()
    
    u = RasterPerimeter('/mnt/data_hdd/snow_and_ice/snow_and_ice_scheduler_full/test_bundle/l2a_full/SENTINEL2A_20180604-103550-770_L2A_T32TLR_C_V1-0/SENTINEL2A_20180604-103550-770_L2A_T32TLR_C_V1-0_ATB_R1.tif')
    print(u.get_lonlat_minmax())
