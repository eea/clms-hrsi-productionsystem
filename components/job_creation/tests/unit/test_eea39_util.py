from datetime import datetime
import os
from os.path import realpath, dirname
import json

# check if environment variable is set, exit in error if it's not
from ....common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

from ....common.python.util.eea39_util import Eea39Util


def test_read_shapefile():
    """Test that EEA39 shapefile is correctly read."""

    # Call the function to test
    read_file = Eea39Util.read_shapefile()

    # Ensure the shapefile content has been parsed
    assert read_file is not None, "Error : Couldn't read shape file!"
    

def test_get_tile_ids():
    """Test that EEA39 tile IDs"""

    # Constant definition
    tile_ids_length = 1054
    first_tile_id = "25SFD"
    last_tile_id = "38TMK"

    # Call the function to test
    tile_ids = Eea39Util.get_tile_ids()

    # Ensure tile id list is correct
    assert len(tile_ids) == tile_ids_length
    assert tile_ids[0] == first_tile_id
    assert tile_ids[-1] == last_tile_id


def test_read_union():
    """Test that union and projection of Sentinel-2 tiles are computed correctly"""

    # Constant definition
    tile_id = "25SFD"
    reference_union_bounds = (
        -31.849127779999947, 
        38.73593643100003, 
        -30.55211502399993, 
        39.74439989600006
        )

    # Two values as it depends on python/Numpy version used
    reference_crs = [
        {'init': 'epsg:4326'}, 
        {'init': 'epsg:9707'}, 
        {'proj': 'longlat', 'datum': 'WGS84', 'geoidgrids': 'us_nga_egm96_15.tif', 'vunits': 'm', 'no_defs': True}
    ]

    # Call the function to test
    union, crs = Eea39Util.read_union(tile_id)

    # Ensure union and crs are computed correctly
    assert union.bounds == reference_union_bounds
    assert crs in reference_crs

def test_get_wkt():
    """Test that EEA39 tile union is correctly converted to WKT format"""

    # Constant definition
    tile_id = "25SFD"
    reference_geometry = "POLYGON Z ((-31.83284204499995 39.74439989600006 0, "\
        "-30.55211502399993 39.72445184100008 0, -30.58623972099997 38.73593643100003 0, "\
        "-31.84912777999995 38.75519833800007 0, -31.83284204499995 39.74439989600006 0))"
      
    # Call the function to test
    geometry = Eea39Util.get_wkt(tile_id)

    # Ensure EEA39 tile union is correctyl converted to WKT format
    assert geometry == reference_geometry

def test_get_simplified_wkt():
    """Test that the simplified WKT geometry of the EEA39 tile union is correctly calculated"""

    # Constant definition
    geometry_length = 122
    first_geometry_element = "MULTIPOLYGON (((46 36"
    last_geometry_element = "-18 26)))"


    # Call the function to test
    geometry = Eea39Util.get_simplified_wkt()

    # convert result to list to analyze it
    geometry_list = geometry.split(",")

    # Ensure the simplified WKT geometry is correct
    assert len(geometry_list) == geometry_length
    assert geometry_list[0] == first_geometry_element
    assert geometry_list[-1] == last_geometry_element


def test_write_geojson():
    """Test that EEA39 tile union is correctly convertedto GeoJSON format"""

    # Constant definition
    output_path =  os.path.join(realpath(dirname(realpath(__file__))), "test_write_geojson.json")
    tile_id = "25SFD"
    reference_content_1 = {
        'type': 'FeatureCollection', 
        'crs': {
            'type': 'name', 
            'properties': {
                'name': 'urn:ogc:def:crs:OGC:1.3:CRS84'
            }
        },
        'features': [{
            'type': 'Feature', 
            'properties': {}, 
            'geometry': {
                'type': 'Polygon', 
                'coordinates': [[[
                    -31.83284204499995, 
                    39.74439989600006, 0.0
                    ], [
                    -30.55211502399993, 
                    39.724451841000075, 0.0
                    ], [
                    -30.58623972099997, 
                    38.73593643100003, 0.0
                    ], [
                    -31.849127779999947, 
                    38.75519833800007, 0.0
                    ], [
                    -31.83284204499995, 
                    39.74439989600006, 0.0
                    ]]]
            }
        }]
    }

    reference_content_2 = {
        'type': 'FeatureCollection', 
        'features': [
            {'type': 'Feature', 
            'properties': {}, 
            'geometry': {
                'type': 'Polygon', 
                'coordinates': [[[
                    -31.83284204499995, 
                    39.74439989600006, 0.0
                    ], [
                    -30.55211502399993, 
                    39.724451841000075, 0.0
                    ], [
                    -30.58623972099997, 38.73593643100003, 0.0
                    ], [
                    -31.849127779999947, 38.75519833800007, 0.0
                    ], [
                    -31.83284204499995, 39.74439989600006, 0.0
                    ]]]
            }
        }]
    }

    reference_content_3 = {
        'type': 'FeatureCollection', 
        'crs': {
            'type': 'name', 
            'properties': {
                'name': 'urn:ogc:def:crs:EPSG::9707'
            }
        },
        'features': [{
            'type': 'Feature', 
            'properties': {}, 
            'geometry': {
                'type': 'Polygon', 
                'coordinates': [[[
                    -31.83284204499995, 
                    39.74439989600006, 0.0
                    ], [
                    -30.55211502399993, 
                    39.724451841000075, 0.0
                    ], [
                    -30.58623972099997, 
                    38.73593643100003, 0.0
                    ], [
                    -31.849127779999947, 
                    38.75519833800007, 0.0
                    ], [
                    -31.83284204499995, 
                    39.74439989600006, 0.0
                    ]]]
            }
        }]
    }

    # Call the function to test
    Eea39Util.write_geojson(output_path, tile_id)

    # Check if ouput file has been generated
    assert os.path.isfile(output_path), \
        "Error : couldn't generate geojson file under '%s'" %output_path

    # Check generated output's content
    if os.path.isfile(output_path):
        with open(output_path, 'r') as f:
            content = json.loads(f.read())
            
        # Ensure gnerated output's content is correct
        assert content in [reference_content_1, reference_content_2, reference_content_3]

        # Remove generated file
        os.remove(output_path)