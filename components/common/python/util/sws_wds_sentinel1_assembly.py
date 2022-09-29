#!/usr/bin/python3

import os
import logging

from datetime import datetime
from yaml import safe_load as yaml_load

from .eea39_util import Eea39Util
from .resource_util import ResourceUtil
from .exceptions import CsiInternalError


'''
https://forum.step.esa.int/t/sentinel-1-grd-preprocessing-graph/17810

FedericoF  Sep '19
Standard workflow for the preprocessing of Sentinel-1 GRDH data.

The GPF graph executes a Sentinel-1 GRDH preprocessing workflow that consists of seven processing steps, applying a series of standard corrections:

Apply Orbit File
Thermal Noise Removal
Border Noise Removal
Calibration
Speckle filtering (optional)
Range Doppler Terrain Correction
Conversion to dB
Sentinel-1 GRD products can be spatially coregistered to Sentinel-2 MSI data grids, in order to promote the use of satellite virtual constellations by means of data fusion techniques. Optionally a speckle filtering can be applied to the input image.


snap forum: lvevi: The noise removal should be done before calibration.

'''


def bb2poly(minX, minY, maxX, maxY):
    # Lazy loading to not create dependencies chain in orchestrator services
    from osgeo import ogr

    # Create ring
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(minX, minY)
    ring.AddPoint(maxX, minY)
    ring.AddPoint(maxX, maxY)
    ring.AddPoint(minX, maxY)
    ring.AddPoint(minX, minY)
    # Create polygon
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


def read_shapefile():
    # Lazy loading to not create dependencies chain in orchestrator services
    from osgeo import ogr

    tiles_utm = ResourceUtil.for_component(
        'job_creation/geometry/eea39_aoi/tiles_utm.yml')

    with open(tiles_utm) as fd:
        tiledict = yaml_load(fd)

    ds = ogr.Open(Eea39Util.SHP_PATH, 0)
    if ds is None:
        raise CsiInternalError(
            "Error",
            "open file failed: " + str(Eea39Util.SHP_PATH)
        )

    shp_srs = ds.GetLayer().GetSpatialRef()

    tile_list = {}
    for i in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(i)
        for fi in range(layer.GetFeatureCount()):
            f = layer.GetFeature(fi)
            fgeo = f.GetGeometryRef()
            g = ogr.CreateGeometryFromWkt(fgeo.ExportToWkt())  # deep copy
            fname = f.GetField("Name")
            td = tiledict[fname]
            epsg = int(td['epsg'])
            utm_g = bb2poly(td['ulx'], td['lry'], td['lrx'], td['uly'])
            tile_list[fname] = {'geometry': g, 'epsg': epsg, 'utm_geometry': utm_g}
    return tile_list


def tilelist2exclude(tiles, dontcross):
    for t in tiles:
        useit = True
        for c in dontcross:
            if t['geometry'].Intersects(c):
                useit = False
        if useit:
            yield(t)


def s1_product_assembly(s1_product_list, tiles=None, pixelsize=60, extdemfile="dem.tif", logger=None):
    '''
    '''
    # Lazy loading to not create dependencies chain in orchestrator services
    from osgeo import ogr, osr

    if logger is None:
        logger = logging.getLogger()

    maxslices = 2
    checkcrossing = True
    s1_done = []
    s1_assembly = dict()
    subst = {}
    subst['locincangl'] = 'false'
    subst['saveSelectedSourceBand'] = 'true'
    subst['PIXELSIZE'] = pixelsize
    extDEMfile = extdemfile
    DEMRESAMPLINGMETHOD = "NEAREST_NEIGHBOUR"

    processingTimeStart = datetime.utcnow()
    processingHost = os.uname()[1]

    tile_list = read_shapefile()

    if tiles:
        # restrict to requested tiles
        tile_list = {k: v for k, v in tile_list.items() if k in tiles}

    res_tiles = []
    res_graph = {}
    res_assembly_info = {}

    for s1 in s1_product_list:
        productIdentifier = s1.product_path
        product_id = s1.product_id
        platform = s1.sentinel1_id.mission
        missionTakeId = str(s1.sentinel1_id.mission_take_id)
        sliceNumber = int(s1.manifest.sliceNumber)
        totalSlices = int(s1.manifest.totalSlices)
        gmlgeometry = s1.other_metadata.gmlgeometry
        startTime = s1.sentinel1_id.start_time
        stopTime = s1.sentinel1_id.stop_time
        relativeOrbitNumber = int(s1.other_metadata.relativeOrbitNumber)
        published = s1.dias_publication_date
        orbitDirection = s1.other_metadata.orbitDirection.upper()

        if sliceNumber != 0:
            s1_slice = "%s_%s_%s" % (platform, str(missionTakeId), str(sliceNumber))
            # check for duplicates
            if s1_slice in s1_done:
                continue
            s1_done.append(s1_slice)
        else:
            s1_slice = "%s_%s_%s" % (platform, str(missionTakeId), startTime.strftime('%Y%m%d%H%M%S'))
            # check for duplicates
            if s1_slice in s1_done:
                continue
            s1_done.append(s1_slice)

        s1_geometry = ogr.CreateGeometryFromGML(gmlgeometry)
        if s1_geometry.GetGeometryName() == "MULTIPOLYGON" and s1_geometry.GetGeometryCount() == 1:
            outer_linearring = s1_geometry.GetGeometryRef(0).GetGeometryRef(0)
        elif s1_geometry.GetGeometryName() == "POLYGON":
            outer_linearring = s1_geometry.GetGeometryRef(0)
        else:
            raise CsiInternalError(
                "Error",
                "invalid geometry: " + str(s1_geometry)
            )

        linear = [outer_linearring.GetPoint(i) for i in range(outer_linearring.GetPointCount())]
        # valid for at least eea39
        if orbitDirection == "ASCENDING":
            line_first_near = min(linear, key=lambda i: i[1])[:2]
            line_first_far = max(linear, key=lambda i: i[0])[:2]
            line_last_far = max(linear, key=lambda i: i[1])[:2]
            line_last_near = min(linear, key=lambda i: i[0])[:2]
            lln, lfn, llf, lff = line_last_near, line_first_near, line_last_far, line_first_far
            ox = (lln[0], lfn[0], llf[0], llf[0])
            oy = (lfn[1], lff[1], lln[1], llf[1])
        elif orbitDirection == "DESCENDING":
            line_first_near = max(linear, key=lambda i: i[0])[:2]
            line_first_far = max(linear, key=lambda i: i[1])[:2]
            line_last_far = min(linear, key=lambda i: i[0])[:2]
            line_last_near = min(linear, key=lambda i: i[1])[:2]
            lln, lfn, llf, lff = line_last_near, line_first_near, line_last_far, line_first_far
            ox = (llf[0], lff[0], lln[0], lfn[0])
            oy = (lln[1], llf[1], lfn[1], lff[1])
        else:
            raise CsiInternalError(
                "Error",
                "unknown orbitDirection: " + str(orbitDirection)
            )

        line_first = ogr.CreateGeometryFromWkt('LINESTRING (%f %f, %f %f)' % (line_first_near[0], line_first_near[1], line_first_far[0], line_first_far[1]))
        line_last  = ogr.CreateGeometryFromWkt('LINESTRING (%f %f, %f %f)' % (line_last_near[0], line_last_near[1], line_last_far[0], line_last_far[1]))

        # check the vertices of the footprint
        if ox[0] > ox[1] or ox[1] > ox[2] or ox[2] > ox[3]:
            logger.warning(f"wrong order of vertices in lon: productIdentifier={productIdentifier}, s1_geometry={s1_geometry}, line_first={line_first}, line_last{line_last}")
            continue
        if oy[0] > oy[1] or oy[1] > oy[2] or oy[2] > oy[3]:
            logger.warning(f"wrong order of vertices in lat: productIdentifier={productIdentifier}, s1_geometry={s1_geometry}, line_first={line_first}, line_last{line_last}")
            continue

        s1product = {
            'product_id': product_id,
            'productIdentifier': productIdentifier,
            'platform': platform,
            'missionTakeId': missionTakeId,
            'sliceNumber': sliceNumber,
            'totalSlices': totalSlices,
            'gmlgeometry': gmlgeometry,
            'startTime': startTime,
            'stopTime': stopTime,
            'published': published,
            'relativeOrbitNumber': relativeOrbitNumber,
            'geometry': s1_geometry,
            'lineFirstGeometry': line_first,
            'lineLastGeometry': line_last,
        }
        s1product['tiles'] = []
        tile_found = False
        # For each tile
        for tile_id, tile in tile_list.items():
            if s1_geometry.Intersects(tile['geometry']):
                tile_found = True
                s1product['tiles'].append(
                    {
                        'geometry': tile['geometry'],   # mb: deep copy
                        'utm_geometry': tile['utm_geometry'],   # mb: deep copy
                        'tile_id': tile_id,
                        'epsg': tile['epsg'],
                    }
                )
                assemblyStripeId = "%s_%s_%d" % (platform, missionTakeId, tile['epsg'])
                if assemblyStripeId not in s1_assembly:
                    s1_assembly[assemblyStripeId] = []
                if s1product not in s1_assembly[assemblyStripeId]:
                    s1_assembly[assemblyStripeId].append(s1product)

    assemblyList = {}
    tileList = {}
    for assemblyStripeId, s1list in s1_assembly.items():
        Anum = 1
        cnt = 0
        prev = None
        assemblyId = None
        for p in sorted(s1list, key=lambda i: i['startTime']):
            if prev and abs((p['startTime'] - prev['startTime']).seconds) <= 1:
                logger.debug(f"skip duplicate slice: {p['product_id']}, using {prev['product_id']}")
                continue
            cnt += 1
            if p['sliceNumber'] == 1 and p['totalSlices'] == 1:
                dontcross = []
            elif p['sliceNumber'] == 1:
                dontcross = [p['lineLastGeometry']]
            elif cnt == 1 and p['sliceNumber'] != 0 and p['sliceNumber'] == p['totalSlices']:
                dontcross = [p['lineFirstGeometry']]
            elif p['sliceNumber'] != 0 and p['sliceNumber'] == p['totalSlices']:
                dontcross = []
            elif cnt == 1:
                dontcross = [p['lineFirstGeometry'], p['lineLastGeometry']]
            else:
                dontcross = [p['lineLastGeometry']]
            if prev and p['sliceNumber'] != 0 and prev['sliceNumber'] != 0 and (p['sliceNumber'] - prev['sliceNumber']) != 1:
                cnt = 1
                Anum += 1
                dontcross.append(p['lineFirstGeometry'])
            elif prev and (p['startTime'] - prev['stopTime']).seconds > 1:
                cnt = 1
                Anum += 1
                dontcross.append(p['lineFirstGeometry'])
            elif prev and cnt >= maxslices:
                assemblyList[assemblyId].append(p)
                tileList[assemblyId].extend(tilelist2exclude(p['tiles'], dontcross if checkcrossing else []))
                cnt = 1
                Anum += 1
                dontcross.append(p['lineFirstGeometry'])
            assemblyId = assemblyStripeId + "_%d" % Anum
            if assemblyId not in assemblyList:
                assemblyList[assemblyId] = []
                tileList[assemblyId] = []
            assemblyList[assemblyId].append(p)
            tileList[assemblyId].extend(tilelist2exclude(p['tiles'], dontcross if checkcrossing else []))
            prev = p

    srs_4326 = osr.SpatialReference()
    srs_4326.ImportFromEPSG(4326)
    #  fix Lat/Lon change : https://github.com/OSGeo/gdal/issues/1546
    if hasattr(srs_4326, "SetAxisMappingStrategy"):
        srs_4326.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    srs_tile = osr.SpatialReference()
    if hasattr(srs_tile, "SetAxisMappingStrategy"):
        srs_tile.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    tileDone = []
    for assemblyAnum, prodList in assemblyList.items():
        assembly_info = {}
        if len(prodList) < 1:
            logger.debug("prodList <" + assemblyAnum + "> empty")
            continue
        (platform, missionTakeId, epsg, Anum) = assemblyAnum.split("_")
        aStartTime = prodList[0]['startTime'].strftime('%Y%m%d%H%M%S')
        aStopTime = prodList[-1]['stopTime'].strftime('%Y%m%d%H%M%S')
        assemblyId = "%s_%s_%s_%s_%s" % (platform, missionTakeId, epsg, aStartTime, aStopTime)
        subst['epsg'] = int(epsg)
        subst['missionTakeId'] = missionTakeId
        subst['assemblyId'] = assemblyId
        assembly_info['epsg'] = subst['epsg']
        assembly_info['s1_productIdentifier'] = []
        assembly_info['s1_product_id'] = []
        for Inum, prod_desc in enumerate(prodList):
            assembly_info['s1_productIdentifier'].append(prod_desc['productIdentifier'])
            assembly_info['s1_product_id'].append(prod_desc['product_id'])

        relativeOrbitNumber = prodList[0]['relativeOrbitNumber']
        tile_found = False
        for tile in tileList[assemblyAnum]:
            if subst['epsg'] != tile['epsg']:
                continue
            tile_id = tile['tile_id']
            bnTile = "wsp_tile_%s_%s_%s" % (platform, subst['missionTakeId'], tile_id)
            if bnTile in tileDone:
                continue
            tileDone.append(bnTile)
            srs_tile.ImportFromEPSG(tile['epsg'])
            transform1 = osr.CoordinateTransformation(srs_4326, srs_tile)
            transform2 = osr.CoordinateTransformation(srs_tile, srs_4326)

            sourceIds = []
            sourceProducts = []
            product_startTime = None
            product_stopTime = None
            sourceProduct_published = None
            tile_sourceProducts_area = 0
            tile_sourceProduct_geometry = ogr.Geometry(ogr.wkbPolygon)
            tile_sourceProduct_utmgeometry = ogr.Geometry(ogr.wkbPolygon)
            for p in assemblyList[assemblyAnum]:
                for i in p['tiles']:
                    if tile_id == i['tile_id']:
                        sourceIds.append(p['product_id'])
                        sourceProducts.append(p['productIdentifier'])
                        if product_startTime is None or product_startTime > p['startTime']:
                            product_startTime = p['startTime']
                        if product_stopTime is None or product_stopTime < p['stopTime']:
                            product_stopTime = p['stopTime']
                        if sourceProduct_published is None or sourceProduct_published < p['published']:
                            sourceProduct_published = p['published']
                        tile_sourceProduct_geometry = tile_sourceProduct_geometry.Union(p['geometry'].Intersection(i['geometry']))
                        p_utm = p['geometry'].Clone()
                        p_utm.Transform(transform1)
                        utm_intersection = p_utm.Intersection(tile['utm_geometry'])
                        tile_sourceProduct_utmgeometry = tile_sourceProduct_utmgeometry.Union(utm_intersection)
                        tile_sourceProducts_area += utm_intersection.Area()

            if tile_sourceProducts_area < 10 * 1E6:
                continue
            tile_found = True

            # print (tile_sourceProduct_geometry.ExportToWkt())
            # tile_sourceProduct_geometry = tile_sourceProduct_utmgeometry.Clone()
            # tile_sourceProduct_geometry.Transform(transform2)
            # print (tile_sourceProduct_geometry.ExportToWkt())
            tile_sourceProduct_geometry.Set3D(False)
            tile_sourceProduct_utmgeometry.Set3D(False)
            tile_yml = {'tile_id': tile_id,
                        'tile_envelope': tile['geometry'].GetEnvelope(),   # (minX, minY, maxX, maxY )
                        'sourceIds': sourceIds,
                        'sourceProducts': sourceProducts,
                        'platform': platform.lower(),
                        'missionTakeId': str(subst['missionTakeId']).lower(),
                        'relativeOrbitNumber': relativeOrbitNumber,
                        'sourceProduct_startTime': product_startTime,
                        'sourceProduct_stopTime': product_stopTime,
                        'sourceProduct_published': sourceProduct_published,
                        'assemblyId': assemblyId,
                        'areaUTM': tile_sourceProducts_area,
                        'sourceGeometry': tile_sourceProduct_geometry.ConvexHull().ExportToWkt(),
                        'sourceGeometrySRID':  ("SRID=%d;" % subst['epsg']) + tile_sourceProduct_utmgeometry.ExportToWkt(),
                        # 'productTimelinessCategory': tile_prod_mindist_prod['productTimelinessCategory'],
                        'processingTime': processingTimeStart.isoformat(),
                        'processingHost': processingHost,
                        }
#            with open(bnTile + ".yml", "w") as f:
#                f.write(yaml_dump(tile_yml, default_flow_style = False, allow_unicode = True, encoding = None))
            res_tiles.append(tile_yml)

        if tile_found:
            res_assembly_info[assemblyId] = assembly_info

    return res_tiles, None, res_assembly_info


def intersectFscS1(fgml, s1wkt):
    # Lazy loading to not create dependencies chain in orchestrator services
    from osgeo import ogr

    fp_union = tile_sourceProduct_geometry = ogr.Geometry(ogr.wkbPolygon)
    s1_geometry = ogr.CreateGeometryFromWkt(s1wkt)
    for f in fgml:
        f1 = ogr.CreateGeometryFromGML(f)
        fp_union = fp_union.Union(s1_geometry.Intersection(f1))
    fp_union.Set3D(False)

    return fp_union.ExportToWkt()


if __name__ == '__main__':
    import argparse
    from yaml import safe_load as yaml_load
    from yaml import dump as yaml_dump

    argparser = argparse.ArgumentParser(description="test functions")
    argparser.add_argument("--s1_product_assembly", help="generate s1 and tile list", action='store_true')
    argparser.add_argument("--args", help="arguments", nargs="*")
    argparser.add_argument("--kwargs", help="arguments", nargs="*")

    args = argparser.parse_args()
    fargs = args.args
    fkwargs = args.kwargs

    if args.s1_product_assembly:
        res_tiles, res_graph, res_assembly_info = s1_product_assembly(yaml_load(open(fargs[0])), fargs[1], int(fargs[2]), fargs[3])
        print(res_tiles)
        print(res_graph)
        print(res_assembly_info)
