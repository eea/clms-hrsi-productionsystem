from builtins import staticmethod
import math
from os import path
import re
from threading import Lock

import geopandas
import pandas
from shapely import geometry
import shapely.wkt

from .file_util import FileUtil
from .log_util import temp_logger
from .resource_util import ResourceUtil


class Eea39Util(object):
    '''
    Utility functions for the EEA39 (European territory) geometry.
    See:
     * https://automating-gis-processes.github.io/2016/Lesson2-geopandas-basics.html
     * https://geopandas.readthedocs.io/en/latest/gallery/create_geopandas_from_pandas.html
     * http://geopandas.org/io.html
    '''

    # Path on disk of the EEA39 shapefile, in WGS84 projection.
    SHP_PATH = ResourceUtil.for_component(
        'job_creation/geometry/eea39_aoi/shp/'
        'S2A_OPER_GIP_TILPAR_MPC__20151209T095117_V20150622T000000_21000101T000000_B00_SnowAndIce_AOI.shp')

    # Shapefile field names
    SHP_TILE_ID = 'Name'
    SHP_GEOMETRY = 'geometry'
    
    __no_rlie_tiles = ['26SLJ', '26SMJ', '26SPF', '26VPR', '27RYN', '28RCU', '28RDU', '28RET', '28SBB', '28SCA', '28UGD', \
        '29SQV', '29TMG', '29TMJ', '29VNC', '29VPF', '30SUD', '30SUE', '30SVD', '30SWD', \
        '30SXF', '30UUV', '31VEF', '31VEK', '31VFL', '32VKK', '32WMS', '33STV', '33SUV', '33SVV', \
        '33TVJ', '33TWH', '33TXG', '34SFE', '34SFF', '34SGD', '34TBM', '34VDK', '34VDL', '34WEE', '35SKD', \
        '35SLC', '35SLD', '35SMU', '35SQV', '35ULP', '35UMV', '35WLV', '35WNV', '36SXE', '37SEA', '37SFA', '37TFG', '38SMF']

    # List of the EEA39 tile IDs, read in the shapefile.
    __tile_ids = None

    # Simplified WKT geometry of the EEA39 tile union, short enough so it can be passed in an URL.
    __simplified_wkt = None

    @staticmethod
    def read_shapefile():
        '''Read and return the EEA39 shapefile.'''

        # Check that the shapefile exists
        if not path.isfile(Eea39Util.SHP_PATH):
            raise Exception(
                "EEA39 shapefile is missing: %s" % Eea39Util.SHP_PATH)

        # Read and return the shapefile
        return geopandas.read_file(Eea39Util.SHP_PATH)
        
    @staticmethod
    def select_rlie_zone_tile_ids(tile_list_in):
        return sorted(list(set(tile_list_in) - set(Eea39Util.__no_rlie_tiles)))
    

    __lock_get_tile_ids = Lock()
    

    @staticmethod
    def get_tile_ids():
        '''Calculate (only once) and return the EEA39 tile IDs.'''

        # No concurrent calls
        with Eea39Util.__lock_get_tile_ids:

            # Already calculated
            if Eea39Util.__tile_ids is not None:
                return Eea39Util.__tile_ids

            # Init list
            Eea39Util.__tile_ids = []

            # Read the shapefile
            shapefile = Eea39Util.read_shapefile()

            # For each shapefile feature
            for _, row in shapefile.iterrows():

                # Save the tile ID
                Eea39Util.__tile_ids.append(row[Eea39Util.SHP_TILE_ID].upper())
                
            return Eea39Util.__tile_ids


    @staticmethod
    def read_union(tile_id=None):
        '''
        Read the union and projection of all the Sentinel-2 tiles from the EEA39 shapefile.

        :param tile_id: If not None, read only this Sentinel-2 tile.
        :return: (union, crs)
        '''

        # Read the shapefile
        shapefile = Eea39Util.read_shapefile()

        # Union of the tile geometries
        union = None

        if tile_id is not None:
            if not isinstance(tile_id, list):
                tile_id = [tile_id]
            tile_id_lower = [id.lower() for id in tile_id]

        # For each shapefile feature
        for _, row in shapefile.iterrows():

            # Not the searched tile ID
            if tile_id is not None:
                current_tile_id = row[Eea39Util.SHP_TILE_ID].lower()
                if current_tile_id.lower() not in tile_id_lower:
                    continue

            # Update union
            tile_geometry = row[Eea39Util.SHP_GEOMETRY]
            if (union is None):
                union = tile_geometry
            else:
                union = union.union(tile_geometry)

        if (union is None) and (tile_id is not None):
            raise Exception(
                'Sentinel-2 tile ID \'%s\' '
                'was not found in the EEA39 shapefile: %s'
                % (tile_id, Eea39Util.SHP_PATH))

        if (union is None):
            raise Exception('Shapefile geometries are empty')

        return (union, shapefile.crs)

    @staticmethod
    def get_wkt(tile_id=None):
        '''
         Convert the EEA39 tile union to WKT format.

        :param tile_id: If not None, write only this Sentinel-2 tile.
        :return: WKT string
        '''
        (union, _) = Eea39Util.read_union(tile_id)
        return union.wkt

    __lock_get_simplified_wkt = Lock()

    @staticmethod
    def get_simplified_wkt(return_geometry=False):
        '''Calculate (only once) the simplified WKT geometry of the EEA39 tile union.'''

        # No concurrent calls
        with Eea39Util.__lock_get_simplified_wkt:

            # Already calculated
            if Eea39Util.__simplified_wkt is not None:
                return Eea39Util.__simplified_wkt

            temp_logger.info('Calculate the simplified WKT geometry of the EEA39 tile union.')

            # Create a cell as a polygon from bounds and step
            def cell_to_polygon(minx, miny, step):
                return geometry.Polygon([
                    [minx, miny],
                    [minx + step, miny],
                    [minx + step, miny + step],
                    [minx, miny + step]])

            def to_short_wkt(geometry):
                '''Convert a geometry to a short WKT.'''

                # Original WKT
                wkt = geometry.wkt

                # Remove the trailing zeros : .00000
                # Warning: does not work in case of e.g. .01000
                pattern = re.compile(r'\.0+([^\.])')
                wkt = pattern.sub(r'\1', wkt)

                # Remove spaces after ,
                wkt = wkt.replace(', ', ',')
                return wkt

            #  Read the union of all the Sentinel-2 tiles from the EEA39 shapefile.
            (original, _) = Eea39Util.read_union()

            # Step #1 : round the geometry coordinates to n decimals,
            # using a grid that contains the geometry.
            # The rounded values are shorter in the WKT string.

            # Grid step in degrees
            GRID_STEP = 1

            # Rounded grid that contains the geometry
            grid = None

            # Rounded bouning box. x=horizontal, y=vertical.
            (minx, miny, maxx, maxy) = original.bounds
            minx = math.floor(minx / GRID_STEP) * GRID_STEP
            miny = math.floor(miny / GRID_STEP) * GRID_STEP
            maxx = math.ceil(maxx / GRID_STEP) * GRID_STEP
            maxy = math.ceil(maxy / GRID_STEP) * GRID_STEP

            # For each cell of the grid
            for x in range(minx, maxx, GRID_STEP):
                for y in range(miny, maxy, GRID_STEP):

                    # Create the cell as a polygon
                    cell = cell_to_polygon(x, y, GRID_STEP)

                    # Keep the cell only if it intersects the geometry
                    if cell.intersects(original):
                        grid = cell if grid is None else grid.union(cell)

            # Step #2 : for each polygon of the multipolygon, split the polygon
            # into several parts and only keep the convex hull for each part.
            # We could use simplify instead but we would lose some geometry surface.

            # Each split size in degrees
            SPLIT_STEP = 5

            # Union of the convex hulls for each polygon
            multi_hull = None

            # For each polygon in the grid
            for grid_polygon in grid:

                # Union of the convex hulls for this polygon
                polygon_hull = None

                # For each cell of the grid
                for x in range(minx, maxx, SPLIT_STEP):
                    for y in range(miny, maxy, SPLIT_STEP):

                        # Create the cell as a polygon
                        cell = cell_to_polygon(x, y, SPLIT_STEP)

                        # Part of the grid polygon that intersects the cell
                        cell = cell.intersection(grid_polygon)

                        # Convex hull for this cell
                        hull = cell.convex_hull

                        # Union for this polygon
                        polygon_hull = hull if polygon_hull is None else polygon_hull.union(hull)

                # Union for the multipolygon
                multi_hull = polygon_hull if multi_hull is None else multi_hull.union(polygon_hull)

            # Check result
            if not multi_hull.contains(original):
                raise Exception(
                    "The resulting geometry does not entirely contains the original geometry.")

            # Save and return result
            Eea39Util.__simplified_wkt = to_short_wkt(multi_hull)
            if return_geometry:
                return multi_hull
            return Eea39Util.__simplified_wkt

    @staticmethod
    def write_geojson(output_path, tile_id=None, simplified=False):
        '''
        Convert the EEA39 tile union to GeoJSON format.

        :param output_path: Path to the output GeoJSON file.
        :param tile_id: If not None, write only this Sentinel-2 tile.
        :param simplified: Only keep the simplified geometry (does not work with tile_id)
        '''

        # Read the union and projection of all the Sentinel-2 tiles from the EEA39 shapefile.
        (union, crs) = Eea39Util.read_union(tile_id)

        # Only keep the simplified geometry
        if simplified:
            union = shapely.wkt.loads(Eea39Util.get_simplified_wkt())

        # Write the output GeoJSON directory
        FileUtil.make_file_dir(output_path)

        # Write file
        geojson = geopandas.GeoDataFrame(
            pandas.DataFrame({}),
            crs=crs,
            geometry=[union])
        geojson.to_file(output_path, driver='GeoJSON')


    @staticmethod
    def get_tiles(tile_restriction=None):
        '''
        List of all the EEA39 Sentinel-2 tile IDs.

        :param tile_restriction: ID of a specific S2 tile, if one want to only focus on it.
        '''

        if tile_restriction is not None and not isinstance(tile_restriction, list):
            tile_restriction = [tile_restriction]

        tile_ids = (
            Eea39Util().get_tile_ids()
            if tile_restriction is None
            else tile_restriction)
        return tile_ids


    @staticmethod
    def get_geometry(tile_restriction=None):
        '''
        EEA39 Area Of Interest (AOI) to request, in WGS84 projection.

        :param tile_restriction: ID of a specific S2 tile, if one want to only focus on it.
        '''

        geometry = (
            Eea39Util.get_simplified_wkt()
            if tile_restriction is None
            else Eea39Util.get_wkt(tile_id=tile_restriction))
        return geometry
