#!/usr/bin/env python3

import os
import argparse
import numpy as np
import logging
from osgeo import gdal, osr
from yaml import dump as yaml_dump
from yaml import safe_load as yaml_load

from sws_wds_snap_template_strings import *


logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)


def readColorTable(color_file):
    color_table = gdal.ColorTable()
    with open(color_file, "r") as fp:
        for line in fp:
            line = line.strip()
            entry = line.split(",")
            if not line.startswith('#') and len(entry) >= 4:
                if len(entry) > 4:
                    alpha = int(entry[4])
                else:
                    alpha = 0
                color_table.SetColorEntry(int(entry[0]), (int(entry[1]), int(entry[2]), int(entry[3]), alpha))
    return color_table


def create_wsm(args):
    # RC_TRESHLIST = [-3.5, -3., -2.5, -2.]
    RC_TRESHLIST = [-4.0, -3.5, -3.0, -2.5]

    CODE_SSC_WET = 110
    CODE_SSC_DRY = 115
    CODE_SSC_SNOWFREE = 120
    CODE_CLOUD = 205
    CODE_NONMONTAIN = 240
    CODE_MASKS = 250
    CODE_NODATA = 255

    CODE_WSM_WET = 110
    CODE_WSM_NOTWET = 125

    theta1 = 20
    theta2 = 45
    k = 0.5

    # treshold in db for noise
    VHDBNOISE = 32
    VVDBNOISE = 30

    # return status
    ret = 0

    # suppress to create .aux.xml files
    os.environ['GDAL_PAM_ENABLED'] = 'NO'
    # output driver
    drv = gdal.GetDriverByName('MEM')
    # copy is needed for Cloud Optimized Geotiff (COG), https://geoexamples.com/other/2019/02/08/cog-tutorial.html/
    drv2 = gdal.GetDriverByName('GTiff')
    optionCOG = ["COMPRESS=DEFLATE", "ZLEVEL=4", "PREDICTOR=1", "TILED=YES", "BLOCKXSIZE=1024", "BLOCKYSIZE=1024",
                 "COPY_SRC_OVERVIEWS=YES"]

    # Read reference for this tile
    ds = gdal.Open(args.ref, gdal.GA_ReadOnly)
    if ds is None:
        logger.error("gdal.Open() failed for file: " + args.ref)
        raise SystemExit(21)
    srs = osr.SpatialReference()
    srs.ImportFromWkt(ds.GetProjectionRef())
    gt = ds.GetGeoTransform()
    refxmin, refxpix, _, refymax, _, refypix = gt
    refxsiz, refysiz = ds.RasterXSize, ds.RasterYSize
    refvhnd = ds.GetRasterBand(1).GetNoDataValue()
    refvvnd = ds.GetRasterBand(2).GetNoDataValue()

    VHref = ds.GetRasterBand(1).ReadAsArray()
    VVref = ds.GetRasterBand(2).ReadAsArray()
    # Convert to dB
    mask_ref = (VHref > 0) & (VVref > 0) & (VHref != refvhnd) & (VVref != refvvnd)
    VHrefdB = 10 * np.log10(VHref, where=mask_ref)
    VVrefdB = 10 * np.log10(VVref, where=mask_ref)

    # Read ifile
    ds = gdal.Open(args.s1ass_file, gdal.GA_ReadOnly)
    if ds is None:
        logger.error("gdal.Open() failed for file: " + args.s1ass_file)
        raise SystemExit(21)
    s1xmin, s1xpix, _, s1ymax, _, s1ypix = ds.GetGeoTransform()
    s1xsiz, s1ysiz = ds.RasterXSize, ds.RasterYSize
    s1vhnd = ds.GetRasterBand(1).GetNoDataValue()
    s1vvnd = ds.GetRasterBand(2).GetNoDataValue()
    # Convert to dB
    S1assVH = ds.GetRasterBand(1).ReadAsArray()
    S1assVV = ds.GetRasterBand(2).ReadAsArray()

    mask_s1 = (S1assVH > 0) & (S1assVV > 0) & (S1assVH != s1vhnd) & (S1assVV != s1vvnd)
    if mask_s1.sum() == 0:
        # no data, return S1 Assembly is empty, we could already exit with 0xD7
        # raise SystemExit(209)
        ret |= 0xD0 | 0x01

    assert refxpix == s1xpix
    assert refypix == s1ypix
    refxmax = refxmin + refxsiz * refxpix
    refymin = refymax + refysiz * refypix
    s1ymin = s1ymax + s1ysiz * s1ypix
    s1xmax = s1xmin + s1xsiz * s1xpix

    # Find top left pixel offset
    pix_xi = int((refxmin - s1xmin) / refxpix) if (refxmin > s1xmin) else 0
    pix_yi = int((refymax - s1ymax) / refypix) if (refymax < s1ymax) else 0

    # Find cut out length
    xend    = s1xmax if s1xmax < refxmax else refxmax
    x_len   = int((xend - (s1xmin + pix_xi * refxpix)) / refxpix)
    yend    = s1ymin if s1ymin > refymin else refymin
    y_len   = int((yend - (s1ymax + pix_yi * refypix)) / refypix)

    if x_len < 1 or y_len < 1:
        logger.debug(f"no overlap. x_len: {x_len} y_len: {y_len}")
        raise SystemExit(0xD0 | 0x02 | 0x04)

    # Find position in output
    pix_xo  = int((s1xmin - refxmin) / refxpix) if (s1xmin > refxmin) else 0
    pix_yo  = int((s1ymax - refymax) / refypix) if (s1ymax < refymax) else 0

    VH = np.zeros((refysiz, refxsiz), dtype=np.float32)
    VV = np.zeros((refysiz, refxsiz), dtype=np.float32)
    VH[pix_yo:pix_yo + y_len, pix_xo:pix_xo + x_len] = S1assVH[pix_yi:pix_yi + y_len, pix_xi:pix_xi + x_len]
    VV[pix_yo:pix_yo + y_len, pix_xo:pix_xo + x_len] = S1assVV[pix_yi:pix_yi + y_len, pix_xi:pix_xi + x_len]

    if args.removenoise:
        vhnoise = 10 ** (-VHDBNOISE / 10)
        vvnoise = 10 ** (-VVDBNOISE / 10)
    else:
        vhnoise = 0
        vvnoise = 0

    mask_s1 = (VH > vhnoise) & (VV > vvnoise) & (VH != s1vhnd) & (VV != s1vvnd)
    VHdB = 10 * np.log10(VH, where=mask_s1)
    VVdB = 10 * np.log10(VV, where=mask_s1)

    # Compute RVV and RVH
    RVH = VHdB - VHrefdB
    RVV = VVdB - VVrefdB

    # Initialise weighting
    ds = gdal.Open(args.incangle, gdal.GA_ReadOnly)
    if ds is None:
        logger.error("gdal.Open() failed for file: " + args.incangle)
        raise SystemExit(21)
    iaxmin, iaxpix, _, iaymax, _, iaypix = ds.GetGeoTransform()
    iaxsiz, iaysiz = ds.RasterXSize, ds.RasterYSize
    iand = ds.GetRasterBand(1).GetNoDataValue()
    IA = ds.GetRasterBand(1).ReadAsArray()
    mask_ia = (IA > -180) & (IA < 180) & (IA != iand)
    mnodata = np.logical_not(mask_ref & mask_s1 & mask_ia)
    if mnodata.sum() == mnodata.shape[0] * mnodata.shape[1]:
        # no data, tiles will be empty
        ret |= 0xD0 | 0x02 | 0x04

    # Compute weighting
    W = np.ones(IA.shape, dtype=np.float32)
    m = (IA >= theta1) & (IA <= theta2) & (IA != iand)
    W[m] = k * (1 + (theta2 - IA[m]) / (theta2 - theta1))
    m = IA > theta2
    W[m] = k

    # Compute Rc
    Rc = W * RVH + (1 - W) * RVV

    wsm = np.empty(VH.shape, dtype=np.uint8)
    wsm[:] = CODE_NODATA
    qcwsm = np.empty(VH.shape, dtype=np.uint8)
    qcwsm[:] = CODE_NODATA

    m = Rc >= RC_TRESHLIST[3]
    wsm[m] = CODE_WSM_NOTWET
    qcwsm[m] = CODE_MASKS

    m = ((Rc >= RC_TRESHLIST[2]) & (Rc < RC_TRESHLIST[3]))
    wsm[m] = CODE_WSM_WET
    qcwsm[m] = 3
    m = ((Rc >= RC_TRESHLIST[1]) & (Rc < RC_TRESHLIST[2]))
    wsm[m] = CODE_WSM_WET
    qcwsm[m] = 2
    m = ((Rc >= RC_TRESHLIST[0]) & (Rc < RC_TRESHLIST[1]))
    wsm[m] = CODE_WSM_WET
    qcwsm[m] = 1
    m = (Rc < RC_TRESHLIST[0])
    wsm[m] = CODE_WSM_WET
    qcwsm[m] = 0

    masks = np.zeros((refysiz, refxsiz), dtype=np.uint8)
    ds = gdal.Open(args.mask, gdal.GA_ReadOnly)
    if ds is None:
        logger.error("gdal.Open() failed for file: " + args.mask)
        raise SystemExit(21)
    bmask = ds.GetRasterBand(1).ReadAsArray()
    m = (bmask != 0)
    masks[m] = bmask[m]
    ds = gdal.Open(args.layshad, gdal.GA_ReadOnly)
    if ds is None:
        logger.error("gdal.Open() failed for file: " + args.layshad)
        raise SystemExit(21)
    bmask = ds.GetRasterBand(1).ReadAsArray()
    m = (bmask != 0)
    masks[m] = bmask[m]

    if args.generate_wds_product:
        ssc = np.array(wsm)
        qcssc = np.array(qcwsm)
        ssc_valid = np.zeros((refysiz, refxsiz), dtype=np.uint8)
        ssc[:] = CODE_NODATA
        qcssc[:] = CODE_NODATA

        for fsc_path in args.fsc_paths:
            ds = gdal.Open(fsc_path, gdal.GA_ReadOnly)
            if ds is None:
                logger.error("gdal.Open() failed for file: " + fsc_path)
                raise SystemExit(21)
            fsc = ds.GetRasterBand(1).ReadAsArray()

            scale_y, scale_x = fsc.shape[0] // ssc.shape[0], fsc.shape[1] // ssc.shape[1]
            observed_th = (scale_y * scale_x) // 2
            for y in range(0, ssc.shape[0]):
                fsc_y = y * scale_y
                for x in range(0, ssc.shape[1]):
                    fsc_x = x * scale_x
                    d = fsc[fsc_y:fsc_y + scale_y, fsc_x:fsc_x + scale_x]
                    observed = ((d >= 0) & (d <= 100))
                    observed_count = np.count_nonzero(observed)
                    cloud = (d == CODE_CLOUD)
                    cloud_count = np.count_nonzero(cloud)
                    cloud_th = scale_y * scale_x - observed_count - cloud_count
                    if observed_count > observed_th:
                        ssc_valid[y, x] = 1
                        # observed
                        if d[observed].mean() >= 90:
                            # snow
                            if wsm[y, x] == CODE_WSM_WET:
                                # wet snow
                                ssc[y, x] = CODE_SSC_WET
                                qcssc[y, x] = qcwsm[y, x]
                            elif wsm[y, x] == CODE_WSM_NOTWET:
                                ssc[y, x] = CODE_SSC_DRY
                                qcssc[y, x] = CODE_MASKS
                        else:
                            # no snow
                            ssc[y, x] = CODE_SSC_SNOWFREE
                            qcssc[y, x] = CODE_MASKS
                    elif cloud_count > cloud_th:
                        ssc_valid[y, x] = 2
                        # cloudy
                        ssc[y, x] = CODE_CLOUD
                        qcssc[y, x] = CODE_MASKS

        m = (ssc_valid == 1) & (masks != 0)
        ssc[m] = masks[m]
        qcssc[m] = CODE_MASKS
        ssc[mnodata] = CODE_NODATA
        qcssc[mnodata] = CODE_NODATA

        if (ssc != CODE_NODATA).sum() == 0:
            # wds is empty
            ret |= 0xD0 | 0x04

        os.makedirs(os.path.dirname(args.ssc), exist_ok=True)
        ds = drv.Create(args.ssc, refxsiz, refysiz, 1, gdal.GDT_Byte)
        if ds is None:
            logger.error("drv.Create() failed for file: " + args.ssc)
            raise SystemExit(22)
        ds.SetGeoTransform(gt)
        ds.SetProjection(srs.ExportToWkt())
        band = ds.GetRasterBand(1)
        band.WriteArray(ssc)
        band.SetNoDataValue(CODE_NODATA)
        if args.ssc_colortable:
            ct = readColorTable(args.ssc_colortable)
            band.SetRasterColorTable(ct)
            band.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '1024')
        ds.BuildOverviews("NEAREST", [2, 4, 8, 16, 32])
        ds2 = drv2.CreateCopy(args.ssc, ds, options=optionCOG)
        if ds2 is None:
            logger.error("drv2.CreateCopy() failed for file: " + args.ssc)
            raise SystemExit(23)
        thumbnail = os.path.join(os.path.dirname(args.ssc), "thumbnail.png")
        ds3 = gdal.Translate(thumbnail, ds, format="PNG", width=1000, height=1000, resampleAlg="nearest")
        if ds3 is None:
            logger.error("gdal.Translate() failed for file: " + thumbnail)
            raise SystemExit(24)
        band = None

        os.makedirs(os.path.dirname(args.qcssc), exist_ok=True)
        ds = drv.Create(args.qcssc, refxsiz, refysiz, 1, gdal.GDT_Byte)
        if ds is None:
            logger.error("drv.Create() failed for file: " + args.qcssc)
            raise SystemExit(22)
        ds.SetGeoTransform(gt)
        ds.SetProjection(srs.ExportToWkt())
        band = ds.GetRasterBand(1)
        band.WriteArray(qcssc)
        band.SetNoDataValue(CODE_NODATA)
        if args.qcssc_colortable:
            ct = readColorTable(args.qcssc_colortable)
            band.SetRasterColorTable(ct)
            band.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '1024')
        ds.BuildOverviews("NEAREST", [2, 4, 8, 16, 32])
        ds2 = drv2.CreateCopy(args.qcssc, ds, options=optionCOG)
        if ds2 is None:
            logger.error("drv2.CreateCopy() failed for file: " + args.qcssc)
            raise SystemExit(23)
        band = None

    if args.generate_sws_product:
        ds = gdal.Open(args.mountain, gdal.GA_ReadOnly)
        if ds is None:
            logger.error("gdal.Open() failed for file: " + args.mountain)
            raise SystemExit(21)
        mountain = ds.GetRasterBand(1).ReadAsArray()
        ds = gdal.Open(args.snowmask, gdal.GA_ReadOnly)
        if ds is None:
            logger.error("gdal.Open() failed for file: " + args.snowmap)
            raise SystemExit(21)
        snowmask = ds.GetRasterBand(1).ReadAsArray()
        m = (masks != 0)
        wsm[m] = masks[m]
        qcwsm[m] = CODE_MASKS
        m = (mountain != 0)
        wsm[m] = CODE_NONMONTAIN
        qcwsm[m] = CODE_MASKS
        m = (snowmask < 30) & (wsm == CODE_WSM_WET)
        wsm[m] = CODE_WSM_NOTWET
        qcwsm[m] = CODE_MASKS
        wsm[mnodata] = CODE_NODATA
        qcwsm[mnodata] = CODE_NODATA

        os.makedirs(os.path.dirname(args.wsm), exist_ok=True)
        ds = drv.Create(args.wsm, refxsiz, refysiz, 1, gdal.GDT_Byte)
        if ds is None:
            logger.error("drv.Create() failed for file: " + args.wsm)
            raise SystemExit(22)
        ds.SetGeoTransform(gt)
        ds.SetProjection(srs.ExportToWkt())
        band = ds.GetRasterBand(1)
        band.WriteArray(wsm)
        band.SetNoDataValue(CODE_NODATA)
        if args.wsm_colortable:
            ct = readColorTable(args.wsm_colortable)
            band.SetRasterColorTable(ct)
            band.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '1024')
        ds.BuildOverviews("NEAREST", [2, 4, 8, 16, 32])
        ds2 = drv2.CreateCopy(args.wsm, ds, options=optionCOG)
        if ds2 is None:
            logger.error("drv2.CreateCopy() failed for file: " + args.wsm)
            raise SystemExit(23)
        thumbnail = os.path.join(os.path.dirname(args.wsm), "thumbnail.png")
        ds3 = gdal.Translate(thumbnail, ds, format="PNG", width=1000, height=1000, resampleAlg="nearest")
        if ds3 is None:
            logger.error("gdal.Translate() failed for file: " + thumbnail)
            raise SystemExit(24)
        band = None

        os.makedirs(os.path.dirname(args.qcwsm), exist_ok=True)
        ds = drv.Create(args.qcwsm, refxsiz, refysiz, 1, gdal.GDT_Byte)
        if ds is None:
            logger.error("drv.Create() failed for file: " + args.qcssc)
            raise SystemExit(22)
        ds.SetGeoTransform(gt)
        ds.SetProjection(srs.ExportToWkt())
        band = ds.GetRasterBand(1)
        band.WriteArray(qcwsm)
        band.SetNoDataValue(CODE_NODATA)
        if args.qcwsm_colortable:
            ct = readColorTable(args.qcwsm_colortable)
            band.SetRasterColorTable(ct)
            band.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
        gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '1024')
        ds.BuildOverviews("NEAREST", [2, 4, 8, 16, 32])
        ds2 = drv2.CreateCopy(args.qcwsm, ds, options=optionCOG)
        if ds2 is None:
            logger.error("drv2.CreateCopy() failed for file: " + args.qcwsm)
            raise SystemExit(23)
        band = None

    return ret


def generate_snap_graph(args):
    from xml.etree import ElementTree as ET

    epsg = args.epsg
    assembly_id = args.assembly_id
    prodList = args.s1_paths
    extDEMfile = args.dem
    pixelsize = args.pixelsize
    snap_out_file = args.s1ass_file

    subst = {}
    subst['locincangl'] = 'false'
    subst['saveSelectedSourceBand'] = 'true'
    subst['PIXELSIZE'] = pixelsize
    if len(prodList) < 1:
        logger.error("prodList <" + assembly_id + "> empty")
        raise SystemExit(11)
    subst['epsg'] = int(epsg)
    subst['assemblyId'] = assembly_id
    pread = []
    xml_graph = ET.fromstring(xml_graph_init)
    for Inum, productIdentifier in enumerate(prodList):
        subst['readId'] = assembly_id + "_%d" % Inum
        # if productIdentifier[:-4] != '.zip':
        #    productIdentifier += '.zip'
        subst['readFile'] = productIdentifier
        subst['readFormat'] = 'SENTINEL-1'
        subst['rmBorderNoiseSourceProduct'] = 'Apply-Orbit-File'
        subst['orbitPolyDegree'] = 2 * len(prodList) + 1
        xml_graph.append(ET.fromstring(xml_graph_read % subst))
        if args.orbit_no:
            subst['rmBorderNoiseSourceProduct'] = 'Read'
        elif args.orbit_prc:
            subst['orbitType'] = 'Precise'
            xml_graph.append(ET.fromstring(xml_graph_orbit % subst))
        else:
            subst['orbitType'] = 'Restituted'
            xml_graph.append(ET.fromstring(xml_graph_orbit % subst))
        xml_graph.append(ET.fromstring(xml_graph_bordernoise % subst))
        xml_graph.append(ET.fromstring(xml_graph_thermalnoiseremoval % subst))
        xml_graph.append(ET.fromstring(xml_graph_calibration % subst))
        pread.append(subst['readId'])
    if len(pread) > 1:
        subst['xml_graph_assembly_source'] = "".join([xml_graph_assembly_source_line % {'Anum': Anum, 'readId': readId} for Anum, readId in enumerate(pread)])
        xml_graph.append(ET.fromstring(xml_graph_assembly % subst))
        xml_graph.append(ET.fromstring(xml_graph_multilook % {'assemblyId': assembly_id, 'multilookSource': "SliceAssembly(%s)" % assembly_id}))
    else:
        xml_graph.append(ET.fromstring(xml_graph_multilook % {'assemblyId': assembly_id, 'multilookSource': "Calibration(%s)" % pread[0]}))

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(subst['epsg'])
    subst['PROJCS'] = srs.ExportToWkt()
    subst['externalDEMFile'] = extDEMfile % subst
    subst['terraincorrection_source'] = "Multilook"
    subst['IMGRESAMPLINGMETHOD'] = "BILINEAR_INTERPOLATION"
    subst['terraincorrectionId'] = assembly_id
    subst['STANDARDGRIDORIGIN'] = -1 * pixelsize / 2
    # subst['DEMRESAMPLINGMETHOD'] = DEMRESAMPLINGMETHOD
    xml_graph.append(ET.fromstring(xml_graph_terraincorrection % subst))
    subst['writeId'] = assembly_id
    subst['writeSource'] = "Terrain-Correction(%s)" % assembly_id
    subst['writeFormat'] = 'GeoTIFF'
    subst['writeFile'] = snap_out_file
    xml_graph.append(ET.fromstring(xml_graph_write % subst))
    snap_graph_file, _ = os.path.splitext(snap_out_file)
    snap_graph_file = snap_graph_file + '.graph.xml'
    os.makedirs(os.path.dirname(snap_graph_file), exist_ok=True)
    with open(snap_graph_file, "w") as fd:
        fd.write(ET.tostring(xml_graph).decode('ascii'))

    return snap_graph_file


def read_parameterfile(args):
    with open(args.parameterfile, "r") as fd:
        params = yaml_load(fd)
    for k, v in params.items():
        if not getattr(args, k, None):
            setattr(args, k, v)
    return args


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('parameterfile', help="options read from parameterfile", nargs='?')
    parser.add_argument('--s1ass_file', help="Sentinel-1 assembled file")
    parser.add_argument('--fsc', help="input FSC file")
    parser.add_argument('--ref', help="reference S1 file")
    parser.add_argument('--incangle', help="incident angle file")
    parser.add_argument('--layshad', help="layover/shadow mask file")
    parser.add_argument('--mask', help="mask file")
    parser.add_argument('--mountain', help="mountain mask file")
    parser.add_argument('--snowmask', help="snow mask file")
    parser.add_argument('--ssc', help="baseline wet/dry snow file")
    parser.add_argument('--qcssc', help="baseline wet snow quality file")
    parser.add_argument('--ssc_colortable', help="colortable baseline wet/dry snow file")
    parser.add_argument('--qcssc_colortable', help="colortable baseline wet/dry snow quality file")
    parser.add_argument('--wsm', help="advanced wet snow file")
    parser.add_argument('--qcwsm', help="advanced wet snow quality file")
    parser.add_argument('--wsm_colortable', help="colortable advanced wet snow file")
    parser.add_argument('--qcwsm_colortable', help="colortable advanced wet snow quality file")
    parser.add_argument('--removenoise', help="apply noise treshold", action='store_true')
    parser.add_argument('--orbit_prc', help="use precise orbits", action='store_true')
    parser.add_argument('--orbit_no', help="dont apply orbit file", action='store_true')

    args = parser.parse_args()

    if args.parameterfile:
        args = read_parameterfile(args)

    ret = 0
    if getattr(args, "s1ass_create", False):
        snap_graph_file = generate_snap_graph(args)
        ret = os.system(args.snap_gpt + " " + args.snap_params + " " + snap_graph_file)
        if ret != 0:
            args.orbit_prc = False
            args.orbit_no = True
            logger.info('dont apply orbit files')
            snap_graph_file = generate_snap_graph(args)
            ret = os.system(args.snap_gpt + " " + args.snap_params + " " + snap_graph_file)
            if ret != 0:
                logger.error(f"GPT (SNAP) processor returned with: {ret}")
                raise SystemExit(12)

    if getattr(args, "generate_sws_product", False) or getattr(args, "generate_wds_product", False):
        ret = create_wsm(args)

    raise SystemExit(ret)
