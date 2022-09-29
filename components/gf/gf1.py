import os, uuid, datetime, shutil, argparse, gfio, xmltodict, json
import numpy as np
import validate_cloud_optimized_geotiff

def detectGaps(data, gapvalues):
    gfio.log("Detecting gaps")
    gap = np.zeros(data.shape,dtype=np.bool)
    for value in gapvalues:
        gap = gap + (data == value)
    return gap

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f','--fsc',action='append',help='Path to the fractional snow cover product')
    parser.add_argument('-w','--wds-sws',action='append',help="Path to the wet snow map product(s)")
    parser.add_argument('-o','--output-dir',help='Output main directory')
    parser.add_argument('-p','--product-title',help='Output product title')
    parser.add_argument('-t','--tmp-dir',help='Temporary storage directory')
    sysargv = vars(parser.parse_args())
    if sysargv['fsc'] is None:
        sysargv['fsc'] = []
    if sysargv['wds_sws'] is None:
        sysargv['wds_sws'] = []

    tmpDir = sysargv['tmp_dir']
    if tmpDir is None:
        tmpDir = os.path.join(baseDir,'tmp')
    # print("GFSC1",sysargv)

    missions = sysargv['product_title'].split('_')[2]
    tileId = sysargv['product_title'].split('_')[3]
    processingBaseline = sysargv['product_title'].split('_')[4]

    gfio.log("Spatial gap filling started")

    NODATA = 255
    FOREST = 220
    WATER = 210
    MOUNTAIN = 0
    CLOUD = 205
    WETSNOW = 110
    noWetSnow = [115,120,125,200,210,220,230,240]
    fscGapvalues = [CLOUD, NODATA]
    wscGapvalues = noWetSnow + [CLOUD, NODATA]
    fscMin, fscMax = (0,100)
    fscClasses = [CLOUD, NODATA]    #order important (snow>CLOUD>NODATA)
    qcMin, qcMax = (0,3)
    qcClasses = [CLOUD, NODATA]
    qfMin, qfMax = (None,None)
    qfClasses = []
    fscRes = 20
    gfRes = 60
    fscShape = (5490,5490)
    gfColorMap = {
        0: (0,0,0,255),
        1: (8,51,112,255),
        2: (10,53,113,255),
        3: (13,55,115,255),
        4: (15,57,116,255),
        5: (18,59,118,255),
        6: (20,61,119,255),
        7: (23,63,121,255),
        8: (25,65,122,255),
        9: (28,67,124,255),
        10: (30,70,125,255),
        11: (33,72,126,255),
        12: (35,74,128,255),
        13: (38,76,129,255),
        14: (40,78,131,255),
        15: (43,80,132,255),
        16: (45,82,134,255),
        17: (48,84,135,255),
        18: (50,86,137,255),
        19: (53,88,138,255),
        20: (55,90,139,255),
        21: (58,92,141,255),
        22: (60,94,142,255),
        23: (63,96,144,255),
        24: (65,98,145,255),
        25: (68,100,147,255),
        26: (70,103,148,255),
        27: (73,105,150,255),
        28: (75,107,151,255),
        29: (78,109,152,255),
        30: (80,111,154,255),
        31: (83,113,155,255),
        32: (85,115,157,255),
        33: (88,117,158,255),
        34: (90,119,160,255),
        35: (93,121,161,255),
        36: (95,123,163,255),
        37: (98,125,164,255),
        38: (100,127,165,255),
        39: (103,129,167,255),
        40: (105,131,168,255),
        41: (108,133,170,255),
        42: (110,135,171,255),
        43: (113,138,173,255),
        44: (115,140,174,255),
        45: (118,142,176,255),
        46: (120,144,177,255),
        47: (123,146,178,255),
        48: (125,148,180,255),
        49: (128,150,181,255),
        50: (130,152,183,255),
        51: (133,154,184,255),
        52: (135,156,186,255),
        53: (138,158,187,255),
        54: (140,160,189,255),
        55: (143,162,190,255),
        56: (145,164,191,255),
        57: (148,166,193,255),
        58: (150,168,194,255),
        59: (153,171,196,255),
        60: (155,173,197,255),
        61: (158,175,199,255),
        62: (160,177,200,255),
        63: (163,179,202,255),
        64: (165,181,203,255),
        65: (168,183,204,255),
        66: (170,185,206,255),
        67: (173,187,207,255),
        68: (175,189,209,255),
        69: (178,191,210,255),
        70: (180,193,212,255),
        71: (183,195,213,255),
        72: (185,197,215,255),
        73: (188,199,216,255),
        74: (190,201,217,255),
        75: (193,203,219,255),
        76: (195,206,220,255),
        77: (198,208,222,255),
        78: (200,210,223,255),
        79: (203,212,225,255),
        80: (205,214,226,255),
        81: (208,216,228,255),
        82: (210,218,229,255),
        83: (213,220,230,255),
        84: (215,222,232,255),
        85: (218,224,233,255),
        86: (220,226,235,255),
        87: (223,228,236,255),
        88: (225,230,238,255),
        89: (228,232,239,255),
        90: (230,234,241,255),
        91: (233,236,242,255),
        92: (235,239,243,255),
        93: (238,241,245,255),
        94: (240,243,246,255),
        95: (243,245,248,255),
        96: (245,247,249,255),
        97: (248,249,251,255),
        98: (250,251,252,255),
        99: (253,253,254,255),
        100: (255,255,255,255),
        205: (123,123,123,255),
        NODATA: (0, 0, 0, 0)
    }
    qcColorMap = {
        0: (93,164,0,255),
        1: (189,189,91,255),
        2: (255,194,87,255),
        3: (255,70,37,255),
        205: (123,123,123,255),
        NODATA: (0, 0, 0, 0)
    }

    scale = gfRes/fscRes
    gfShape = tuple(map(int,(fscShape[0]/scale,fscShape[1]/scale)))
    if int(scale) != scale or int(fscShape[0]/scale) != fscShape[0]/scale or int(fscShape[1]/scale) != fscShape[1]/scale:
        gfio.log("Resolutions not compliant. Bad results are expected.")
    scale = int(scale)

    productDirs = []
    productTypes = []
    productTitles = []
    productTimeStamps = []
    productXmls = []
    productStartDates = []
    productEndDates = []
    for productTitle in sysargv['fsc'] + sysargv['wds_sws']:
        try:
            productType = gfio.getProductType(productTitle)
            productDir = gfio.getDirPath(productTitle)
            productTimeStamp = datetime.datetime.strptime(productTitle.split('_')[1].split('-')[0],"%Y%m%dT%H%M%S")
            xmlFile = gfio.getFilePath(productTitle,'MTD.xml')
            productXml = xmltodict.parse(open(xmlFile,'r').read())
            productStartDate = productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:temporalElement']['gmd:EX_TemporalExtent']['gmd:extent']['gml:TimePeriod']['gml:beginPosition']
            try:
                productStartDate = datetime.datetime.strptime(productStartDate,"%Y-%m-%dT%H:%M:%S.%f")
            except:
                productStartDate = datetime.datetime.strptime(productStartDate,"%Y-%m-%dT%H:%M:%S")
            productEndDate = productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:temporalElement']['gmd:EX_TemporalExtent']['gmd:extent']['gml:TimePeriod']['gml:endPosition']
            try:
                productEndDate = datetime.datetime.strptime(productEndDate,"%Y-%m-%dT%H:%M:%S.%f")
            except:
                productEndDate = datetime.datetime.strptime(productEndDate,"%Y-%m-%dT%H:%M:%S")
            productDirs.append(productDir)
            productTypes.append(productType)
            productTitles.append(productTitle)
            productTimeStamps.append(productTimeStamp)
            productXmls.append(productXml)
            productStartDates.append(productStartDate)
            productEndDates.append(productEndDate)
        except Exception as e:
            gfio.log("Problem reading product list")
            gfio.log(e)
            return 32

    if productDirs == []:
        gfio.log("Empty product list")
        return 33

    productOrder = (np.argsort(productTimeStamps)[::-1]).tolist()
    productTitle = sysargv['product_title']
    productDate = datetime.datetime.strptime(sysargv['product_title'].split('_')[1],'%Y%m%d')
    productDir = os.path.join(sysargv['output_dir'],productTitle)

    if os.path.exists(productDir):
        gfio.log("Product already produced for the day. Checking input data")
        xmlFile = gfio.getFilePath(productTitle,'MTD.xml')
        if xmlFile is None:
            gfio.log("Product not complete. Reprocessing.")
            shutil.rmtree(productDir)
        else:
            productXml = xmltodict.parse(open(xmlFile,'r').read())
            productInputTitles = []
            for productInputXml in productXml['gmd:MD_Metadata']['gmd:series']['gmd:DS_OtherAggregate']['gmd:seriesMetadata']:
                productInputTitle = productInputXml['gmd:MD_Metadata']['gmd:fileIdentifier']['gco:CharacterString']
                productInputTitles.append(productInputTitle)
            inputDataMatches = True
            for productInputXml in productXmls:
                if productInputXml['gmd:MD_Metadata']['gmd:fileIdentifier']['gco:CharacterString'] in productInputTitles:
                    continue
                inputDataMatches = False
                break
        if inputDataMatches:
            gfio.log("Input products are same. Returning product.")
            return 0
        else:
            gfio.log("Not the same product. Reprocessing.")
            shutil.rmtree(productDir)

    gf = NODATA*np.ones(shape=gfShape,dtype=np.uint8)
    qc_gf = NODATA*np.ones(shape=gfShape,dtype=np.uint8)
    qf_gf = np.zeros(shape=gfShape,dtype=np.uint8)
    ad_gf = np.zeros(shape=gfShape,dtype=np.uint32)
    success_gf = False
    wsc = NODATA*np.ones(shape=gfShape,dtype=np.uint8)
    qc_wsc = NODATA*np.ones(shape=gfShape,dtype=np.uint8)
    ad_wsc = np.zeros(shape=gfShape,dtype=np.uint32)
    success_wsc = False
    for p in productOrder:
        try:
            if productTypes[p] == 'FSC':
                gfSub, geoTransform, projectionRef = gfio.readRaster(gfio.getFilePath(productTitles[p],'FSCOG.tif'))
                qcSub = gfio.readRaster(gfio.getFilePath(productTitles[p],'QCOG.tif'))[0]
                qfSub = gfio.readRaster(gfio.getFilePath(productTitles[p],'QCFLAGS.tif'))[0]
                gfSub = gfio.upscale(gfSub, scale, NODATA, fscMin, fscMax, fscClasses)
                qcSub = gfio.upscale(qcSub, scale, NODATA, qcMin, qcMax, qcClasses)
                qfSub = gfio.upscale(qfSub, scale, NODATA, qfMin, qfMax, qfClasses)
                adSub = int(productStartDates[p].timestamp())*np.ones(shape=gfShape,dtype=np.uint32)
                if success_gf:
                    gap = detectGaps(gf,fscGapvalues)*~detectGaps(gfSub,fscGapvalues)
                else:
                    gap = np.ones(shape=gfShape,dtype=np.bool)
                np.copyto(gf,gfSub,where=gap)
                np.copyto(qc_gf,qcSub,where=gap)
                np.copyto(qf_gf,qfSub,where=gap)
                np.copyto(ad_gf,adSub,where=gap)
                success_gf = True
            if productTypes[p] in ['WDS','SWS']:
                if productTypes[p] == 'WDS':
                    fileId = 'SSC'
                if productTypes[p] == 'SWS':
                    fileId = 'WSM'
                wscSub, geoTransform, projectionRef = gfio.readRaster(gfio.getFilePath(productTitles[p],fileId+'.tif'))
                qcSub = gfio.readRaster(gfio.getFilePath(productTitles[p],'QC'+fileId+'.tif'))[0]
                adSub = int(productStartDates[p].timestamp())*np.ones(shape=gfShape,dtype=np.uint32)
                if success_wsc:
                    gap = detectGaps(wsc,wscGapvalues)*~detectGaps(wscSub,wscGapvalues)
                else:
                    gap = np.ones(shape=gfShape,dtype=np.bool)
                np.copyto(wsc,wscSub,where=gap)
                np.copyto(qc_wsc,qcSub,where=gap)
                np.copyto(ad_wsc,adSub,where=gap)
                success_wsc = True
        except Exception as e:
            print("Problem in processing ", productTitles[p])
            gfio.log(e)
            return 131

    fuwFile = gfio.getFilePath('FUW___'+gfio.getTile(productTitles[0]))
    fuw = gfio.readRaster(fuwFile)[0]

    if np.sum(detectGaps(gf,fscGapvalues)) != 0 and np.sum(wsc==WETSNOW) != 0:
        # snow status derived
        gap = detectGaps(gf,fscGapvalues)*(wsc==WETSNOW)
        gapCount = np.sum(gap)
        if gapCount != 0:
            gfio.log('SSD gap filling started. Number of gap pixels to be filled:',gapCount)
            np.place(gf,gap,fscMax)
            np.copyto(qc_gf,qc_wsc,where=gap)
            qf_gf = gfio.setBit(qf_gf,6,1,where=gap)
            np.copyto(ad_gf,ad_wsc,where=gap)
            gfio.log('SSD gap filling ended. Number of gap pixels filled:',gapCount - np.sum(detectGaps(gf,fscGapvalues)*(wsc==WETSNOW)))

        #model derived
        # gap = detectGaps(gf,fscGapvalues)
        # nmFile = gfio.getFilePath('NM___'+gfio.getTile(productTitles[0]))
        # if os.path.exists(nmFile):
        #     nm = gfio.readRaster(nmFile)[0]
        # else:
        #     gfio.log('Non-mountain mask not found. Not a mountainous tile.')
        #     nm = np.ones(gap.shape,dtype=np.uint8)
        # mountain = nm == MOUNTAIN
        # nonforest = ~(fuw == FOREST)

        # gapToFollow = gap*mountain*nonforest
        # gapToFill = gap*mountain*nonforest
        # gapCount = np.sum(gapToFill)
        # if gapCount != 0:
        #     gfio.log('MD gap filling started. Number of gap pixels to be filled:',gapCount)
        #     demFile = gfio.getFilePath('DEM___'+gfio.getTile(productTitles[0]))
        #     elev = gfio.readRaster(demFile)[0]
        #     slope = np.zeros((8,gfShape[0],gfShape[1]),np.int32)
        #     slope[0][:,:-1] = elev[:,1:]-elev[:,:-1]    #right
        #     slope[1][:,1:] = elev[:,:-1]-elev[:,1:]     #left
        #     slope[2][:-1,:] = elev[1:,:]-elev[:-1,:]    #down
        #     slope[3][1:,:] = elev[:-1,:]-elev[1:,:]     #up
        #     slope[4][:-1,:-1] = elev[1:,1:]-elev[:-1,:-1]   #rightdown
        #     slope[5][1:,:-1] = elev[:-1,1:]-elev[1:,:-1]   #rightup
        #     slope[6][:-1,1:] = elev[1:,:-1]-elev[:-1,1:]    #leftdown
        #     slope[7][1:,1:] = elev[:-1,:-1]-elev[1:,1:]     #leftup
        #     isAbove = slope > 0                             #8,r,c
        #     upVectors = np.zeros((gfShape[0],gfShape[1],8,2),dtype=np.int8) #r,c,8,2
        #     vectors = np.array([[0,1],[0,-1],[1,0],[-1,0],[1,1],[-1,1],[1,-1],[-1,-1]],dtype=np.int8)
        #     upVectors[:,:] = vectors        #r,c,8,2
        #     upVectors = upVectors.transpose(3,2,0,1)    #2,8,r,c
        #     upVectors[0] *= isAbove
        #     upVectors[1] *= isAbove
        #     upVectors = upVectors.transpose(1,0,2,3)    #8,2,r,c
        #     index = np.indices(gf.shape,dtype=np.uint16)    #r,c
        #     gfio.log("Curating pixels")
        #     count = 0
        #     while count != np.sum(wsc==WETSNOW):
        #         wetMask = (wsc==WETSNOW)
        #         count = np.sum(wetMask)
        #         wetToBeMask = np.zeros((gfShape[0],gfShape[1]),dtype=np.bool)
        #         for d in range(8):
        #             r = index[0] + wetMask*upVectors[d][0]*mountain         # lookup
        #             c = index[1] + wetMask*upVectors[d][1]*mountain
        #             r = index[0] + wetMask*upVectors[d][0]*gapToFollow[r,c]   #only if the upper part is gap and nonforest too
        #             c = index[1] + wetMask*upVectors[d][1]*gapToFollow[r,c]
        #             wetToBeMask[r,c] = np.ones(gfShape,dtype=np.bool)*(r!=index[0])+(c!=index[1])
        #             np.place(wsc,wetToBeMask,WETSNOW)
        #             qc_wsc[r,c] = qc_wsc
        #             ad_wsc[r,c] = ad_wsc

        #     gfio.log("Filling rasters")
        #     gap = gapToFill*(wsc==WETSNOW)
        #     np.place(gf,gap,fscMax)
        #     np.copyto(qc_gf,qc_wsc,where=gap)
        #     qf_gf = gfio.setBit(qf_gf,6,1,where=gap)
        #     qf_gf = gfio.setBit(qf_gf,7,1,where=gap)
        #     np.copyto(ad_gf,ad_wsc,where=gap)

        #     gfio.log('MD gap filling ended. Number of gap pixels filled:',gapCount - np.sum(detectGaps(gf,fscGapvalues)*mountain*nonforest))

    # Fill water pixels in QF
    wm = fuw == WATER
    qf_gf = gfio.setBit(qf_gf,2,0,where=~wm)
    qf_gf = gfio.setBit(qf_gf,2,1,where=wm)

    productTmpDir = os.path.join(tmpDir,str(uuid.uuid4()))
    os.makedirs(productTmpDir)
    geoTransform = (geoTransform[0], gfRes, geoTransform[2], geoTransform[3], geoTransform[4], -gfRes)
    productStartDate = min(productStartDates)
    productEndDate = max(productEndDates)
    try:
        gfio.writeRaster(os.path.join(productTmpDir,gfio.getFileName(productTitle,'GF.tif')),gf,geoTransform,projectionRef,gfColorMap)
        cogCheck = validate_cloud_optimized_geotiff.main(['',os.path.join(productTmpDir,gfio.getFileName(productTitle,'GF.tif')),'-q'])
        if cogCheck == 1:
            gfio.log("COG validation failed for GF.tif")
            return 123
        resourceSize = os.path.getsize(os.path.join(productTmpDir,gfio.getFileName(productTitle,'GF.tif')))
        gfio.writeThumbnail(os.path.join(productTmpDir,'thumbnail.png'),gf,gfColorMap)
        gfio.writeRaster(os.path.join(productTmpDir,gfio.getFileName(productTitle,'QC.tif')),qc_gf,geoTransform,projectionRef,qcColorMap)
        cogCheck = validate_cloud_optimized_geotiff.main(['',os.path.join(productTmpDir,gfio.getFileName(productTitle,'QC.tif')),'-q'])
        if cogCheck == 1:
            gfio.log("COG validation failed for QC.tif")
            return 123
        resourceSize += os.path.getsize(os.path.join(productTmpDir,gfio.getFileName(productTitle,'QC.tif')))
        gfio.writeRaster(os.path.join(productTmpDir,gfio.getFileName(productTitle,'QCFLAGS.tif')),qf_gf,geoTransform,projectionRef)
        cogCheck = validate_cloud_optimized_geotiff.main(['',os.path.join(productTmpDir,gfio.getFileName(productTitle,'QCFLAGS.tif')),'-q'])
        if cogCheck == 1:
            gfio.log("COG validation failed for QCFLAGS.tif")
            return 123
        resourceSize += os.path.getsize(os.path.join(productTmpDir,gfio.getFileName(productTitle,'QCFLAGS.tif')))
        gfio.writeRaster(os.path.join(productTmpDir,gfio.getFileName(productTitle,'AT.tif')),ad_gf,geoTransform,projectionRef)
        cogCheck = validate_cloud_optimized_geotiff.main(['',os.path.join(productTmpDir,gfio.getFileName(productTitle,'AT.tif')),'-q'])
        if cogCheck == 1:
            gfio.log("COG validation failed for AT.tif")
            return 123
        resourceSize += os.path.getsize(os.path.join(productTmpDir,gfio.getFileName(productTitle,'AT.tif')))
    except Exception as e:
        gfio.log("Problem in writing rasters")
        gfio.log(e)
        if os.path.exists(productTmpDir):
            shutil.rmtree(productTmpDir)
        return 121

    productionTime = datetime.datetime.utcnow()
    geometry = gfio.getAlphashape(gf,NODATA, geoTransform, projectionRef)
    gfio.log("Writing JSON Metadata")
    product = {
        # "collection_name": "HR-S&I",
        "resto": {
            # "type": "Feature",
            "geometry": {
                "wkt": geometry
            },
            "properties": {
                # "productIdentifier": "/HRSI/CLMS/Pan-European/High_Resolution_Layers/Snow/GFSC/"+productTimeStamp.strftime("%Y/%m/%d/")+productTitle,
                # "title": productTitle,
                "resourceSize": str(resourceSize), ##Byte
                # "organisationName": "EEA",
                "startDate": productDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), ##sensing start
                "endDate": productEndDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), ##sensing end
                "completionDate": productionTime.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), ##generation date of the product
                # "published": None,
                # "productType": "GFSC",
                # "mission": "S1-S2",
                "resolution": gfRes, ##meters
                "cloudCover": str(int(np.rint(100*np.mean(gf==CLOUD)))), #%
                # "processingBaseline": processingBaseline,
                # "host_base": "cf2.cloudferro.com:8080",
                # "s3_bucket": "HRSI",
                # "thumbnail": "Preview/CLMS/Pan-European/High_Resolution_Layers/Snow/GFSC/"+productTimeStamp.strftime("%Y/%m/%d/")+productTitle+"/thumbnail.png"
            }
        }
    }
    jsonFile = open(os.path.join(productTmpDir,'dias_catalog_submit.json'), 'w')
    json.dump(product, jsonFile)
    jsonFile.close()

    gfio.log("Writing XML Metadata")
    geometryBounds = gfio.getGeoBounds(gf,geoTransform,projectionRef)
    productXml = xmltodict.parse(open(os.path.join(baseDir,'GFSC_Metadata.xml'),'r').read())
    productXml['gmd:MD_Metadata']['gmd:fileIdentifier']['gco:CharacterString'] = productTitle
    productXml['gmd:MD_Metadata']['gmd:contact']['gmd:CI_ResponsibleParty']['gmd:contactInfo']['gmd:CI_Contact']['gmd:address']['gmd:CI_Address']['gmd:electronicMailAddress']['gco:CharacterString'] = 'copernicus@eea.europa.eu'
    productXml['gmd:MD_Metadata']['gmd:dateStamp']['gco:DateTime'] = productionTime.strftime("%Y-%m-%dT%H:%M:%S.%f")
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:citation']['gmd:CI_Citation']['gmd:alternateTitle']['gco:CharacterString'] = productTitle
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:citation']['gmd:CI_Citation']['gmd:date']['gmd:CI_Date']['gmd:date']['gco:DateTime'] = productionTime.strftime("%Y-%m-%dT%H:%M:%S.%f")
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:citation']['gmd:CI_Citation']['gmd:edition']['gco:CharacterString'] = processingBaseline
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:citation']['gmd:CI_Citation']['gmd:editionDate']['gco:DateTime'] = productionTime.strftime("%Y-%m-%dT%H:%M:%S.%f")
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:citation']['gmd:CI_Citation']['gmd:identifier']['gmd:RS_Identifier']['gmd:code']['gco:CharacterString'] = productTitle
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:citation']['gmd:CI_Citation']['gmd:otherCitationDetails']['gco:CharacterString'] = 'https://land.copernicus.eu/user-corner/technical-library'
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:pointOfContact']['gmd:CI_ResponsibleParty']['gmd:contactInfo']['gmd:CI_Contact']['gmd:address']['gmd:CI_Address']['gmd:electronicMailAddress']['gco:CharacterString'] = 'copernicus@eea.europa.eu'
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:geographicElement']['gmd:EX_GeographicBoundingBox']['gmd:westBoundLongitude']['gco:Decimal'] = str(geometryBounds[0])
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:geographicElement']['gmd:EX_GeographicBoundingBox']['gmd:eastBoundLongitude']['gco:Decimal'] = str(geometryBounds[1])
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:geographicElement']['gmd:EX_GeographicBoundingBox']['gmd:southBoundLatitude']['gco:Decimal'] = str(geometryBounds[2])
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:geographicElement']['gmd:EX_GeographicBoundingBox']['gmd:northBoundLatitude']['gco:Decimal'] = str(geometryBounds[3])
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:temporalElement']['gmd:EX_TemporalExtent']['gmd:extent']['gml:TimePeriod']['@gml:id'] = productTitle
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:temporalElement']['gmd:EX_TemporalExtent']['gmd:extent']['gml:TimePeriod']['gml:beginPosition'] = productStartDate.strftime("%Y-%m-%dT%H:%M:%S.%f")
    productXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:temporalElement']['gmd:EX_TemporalExtent']['gmd:extent']['gml:TimePeriod']['gml:endPosition'] = productEndDate.strftime("%Y-%m-%dT%H:%M:%S.%f")
    productXml['gmd:MD_Metadata']['gmd:dataQualityInfo']['gmd:DQ_DataQuality']['gmd:report'][0]['gmd:DQ_NonQuantitativeAttributeAccuracy']['gmd:result']['gmd:DQ_ConformanceResult']['gmd:specification']['gmd:CI_Citation']['gmd:title']['gco:CharacterString'] = productXml['gmd:MD_Metadata']['gmd:dataQualityInfo']['gmd:DQ_DataQuality']['gmd:report'][0]['gmd:DQ_NonQuantitativeAttributeAccuracy']['gmd:result']['gmd:DQ_ConformanceResult']['gmd:specification']['gmd:CI_Citation']['gmd:title']['gco:CharacterString'].replace('[VALIDATION_REPORT_FILENAME]','hrsi-snow-qar') #update when report is ready
    productXml['gmd:MD_Metadata']['gmd:dataQualityInfo']['gmd:DQ_DataQuality']['gmd:report'][0]['gmd:DQ_NonQuantitativeAttributeAccuracy']['gmd:result']['gmd:DQ_ConformanceResult']['gmd:specification']['gmd:CI_Citation']['gmd:date']['gmd:CI_Date']['gmd:date']['gco:Date'] = '1900-01-01' #update when report is ready
    productXml['gmd:MD_Metadata']['gmd:series']['gmd:DS_OtherAggregate']['gmd:seriesMetadata'] = []
    for p in np.argsort(productTimeStamps).tolist():
        productInputTitle = productTitles[p]
        xmlFile = gfio.getFilePath(productInputTitle,'MTD.xml')
        productInputXml = xmltodict.parse(open(xmlFile,'r').read())
        productInputXml['gmd:MD_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:extent']['gmd:EX_Extent']['gmd:temporalElement']['gmd:EX_TemporalExtent']['gmd:extent']['gml:TimePeriod']['@gml:id'] = productInputTitle
        productXml['gmd:MD_Metadata']['gmd:series']['gmd:DS_OtherAggregate']['gmd:seriesMetadata'].append(productInputXml)
    productXml = xmltodict.unparse(productXml, pretty=True)
    xmlFile = open(os.path.join(productTmpDir,gfio.getFileName(productTitle,'MTD.xml')), 'w')
    xmlFile.write(productXml)
    xmlFile.close()

    try:
        shutil.copytree(productTmpDir,productDir)
    except Exception as e:
        gfio.log("Problem in copying product")
        gfio.log(e)
        if os.path.exists(productTmpDir):
            shutil.rmtree(productTmpDir)
        if os.path.exists(productDir):
            shutil.rmtree(productDir)
        return 122

    if os.path.exists(productTmpDir):
        shutil.rmtree(productTmpDir)

    return 0

baseDir = os.path.split(os.path.realpath(os.sys.argv[0]))[0]

if __name__ == "__main__":
    main()
