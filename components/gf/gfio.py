import gdal, os, alphashape, datetime, argparse, yaml
from shapely.geometry import Polygon, LineString, Point
import numpy as np
import mahotas as mh
from osgeo import osr

def log(*message):
        message = map(str,message)
        message = ''.join(message)
        print(message)
        if not os.path.exists(logDir):
            os.makedirs(logDir)
        f = open(os.path.join(logDir,'csi_si_software.log'),'a')
        f.write(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
        f.write(': ')
        f.write(message)
        f.write('\n')
        f.close()

def getProductType(title):
    return title.split('_')[0]

def getTile(title):
    return title.split('_')[3]

def getProductDate(title):
    productDate = title.split('_')[1]
    if '-' in productDate:
        productDate = productDate.split('-')[0]
    if 'T' in productDate:
        productDate = productDate.split('T')[0]
    productDate = datetime.datetime.strptime(productDate,"%Y%m%d")
    productDate = datetime.date(productDate.year,productDate.month,productDate.day)
    return productDate

def getFileName(title,id):
    type = getProductType(title)
    tile = getTile(title)
    if type in ['DEM','FUW','NM']:
        fileName = eval(type.lower()+'File')
    if type in ['FSC','WDS','SWS','GFSC1','GFSC']:
        fileName = title + '_' + id
    if id == 'thumbnail.png':
        fileName = 'thumbnail.png'
    return fileName

def getDirPath(title):
    type = getProductType(title)
    if type in ['FSC','WDS','SWS']:
        productDir = eval(type.lower()+'Dir')
    if type in ['DEM','FUW','NM']:
        productDir = auxDir
    if type == 'GFSC':
        productDir = gfDir
    if type == 'GFSC1':
        productDir = gf1Dir
    filePath = os.path.join(productDir,title)
    return filePath

def getFilePath(title,id=None):
    filePath = os.path.join(getDirPath(title),getFileName(title,id))
    return filePath

def readRaster(fname):
    log("Reading ",fname)
    try:
        gtif = gdal.Open(fname)
        data = gtif.GetRasterBand(1)
        data_shape = (data.YSize,data.XSize)
        data = np.array(data.ReadAsArray())
        geoTransform = gtif.GetGeoTransform()
        projectionRef = gtif.GetProjectionRef()
        return (data,geoTransform,projectionRef)
    except:
        log("Problem in reading ", fname)
        return None

def writeRaster(fname,rasterData, geoTransform, projectionRef, colorMap = None):
    log("Writing into file")
    if rasterData.dtype == np.uint8:
        dtype = gdal.GDT_Byte
        noData = 255
    if rasterData.dtype == np.int16:
        dtype = gdal.GDT_Int16
        noData = 32767
    if rasterData.dtype == np.uint32:
        dtype = gdal.GDT_UInt32
        noData = 4294967295
    if rasterData.dtype == np.int32:
        dtype = gdal.GDT_Int32
        noData = 2147483647
    dst_ds = gdal.GetDriverByName('MEM').Create('', rasterData.shape[0], rasterData.shape[1], 1, dtype)
    dst_ds.SetGeoTransform(geoTransform)
    dst_ds.SetProjection(projectionRef)
    dst_ds.GetRasterBand(1).WriteArray(rasterData)
    dst_ds.GetRasterBand(1).SetNoDataValue(noData)
    band = dst_ds.GetRasterBand(1)
    if colorMap is not None:
        colors = gdal.ColorTable()
        for value in colorMap:
            colors.SetColorEntry(value, colorMap[value])
        band.SetRasterColorTable(colors)
        band.SetRasterColorInterpretation(gdal.GCI_PaletteIndex)
    gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
    gdal.SetConfigOption('GDAL_TIFF_OVR_BLOCKSIZE', '1024')
    dst_ds.BuildOverviews("NEAREST", [2,4,8,16,32])
    band = None
    options = ['COMPRESS=DEFLATE', 'PREDICTOR=1', 'ZLEVEL=4', 'TILED=YES', 'BLOCKXSIZE=1024', 'BLOCKYSIZE=1024', "COPY_SRC_OVERVIEWS=YES"]
    dst_ds2 = gdal.GetDriverByName('GTiff').CreateCopy(fname, dst_ds, options=options)
    dst_ds = None
    dst_ds2 = None
    return True

def writeThumbnail(fname,rasterData,colorMap=None):
    if colorMap is None:
        mh.imsave(fname,rasterData)
        return True
    r,c = np.indices((1000,1000),dtype=np.float64)
    r = (r*rasterData.shape[0]/1000.).astype(np.int32)
    c = (c*rasterData.shape[1]/1000.).astype(np.int32)
    rasterData = rasterData[r,c]
    imageData = np.zeros(rasterData.shape,dtype=np.uint8)
    imageData = np.dstack((imageData,imageData,imageData,imageData)).transpose(2,0,1)
    for value in colorMap:
        for c in range(4):
            np.place(imageData[c],rasterData==value,colorMap[value][c])
    imageData = imageData.transpose(1,2,0)
    mh.imsave(fname,imageData)
    return True

def upscale(data,scale,noData=None,valueMin=None,valueMax=None, classes = []):
    if scale == 1:
        return data
    if valueMin is None:
        valueMax is None
    log("Upscaling raster")
    newShape = tuple(map(int,(data.shape[0]/scale,data.shape[1]/scale)))
    rind = np.indices(newShape)[0]*scale
    cind = np.indices(newShape)[1]*scale

    rowi = rind
    coli = cind
    for j in range(1,scale):
        rowi = np.dstack((rowi,rind))
        coli = np.dstack((coli,cind))
    row = rowi
    col = coli

    for i in range(1,scale):
        rowi = rind+i
        coli = cind+i
        for j in range(1,scale):
            rowi = np.dstack((rowi,rind+i))
            coli = np.dstack((coli,cind+i))
        row = np.dstack((row,rowi))
        col = np.dstack((col,coli))
    if noData is None and valueMin is None and valueMax is None and classes == []:
        dataValue = np.copy(data).astype(np.float64)
        newData = np.mean(dataValue[row,col],axis=2)
        newData = newData.astype(data.dtype)
        return newData
    else:
        dataClass = np.copy(data)
        if valueMax is not None:
            dataValue = np.copy(data).astype(np.float64)
            valueMask = (dataValue >= valueMin)*(dataValue <= valueMax)
            np.place(dataValue,~valueMask,np.nan)
            newDataValue = np.nanmean(dataValue[row,col],axis=2)
            np.place(newDataValue,np.isnan(newDataValue),noData)
            newDataValue = np.rint(newDataValue).astype(data.dtype)
            np.place(dataClass,valueMask,valueMax)

        if classes != []:
            newDataClass = noData*np.ones(shape=newShape,dtype=data.dtype)
            classCount = np.zeros(shape=newShape,dtype=np.int32)
            classValues = classes
            if valueMax is not None:
                count = np.sum(dataClass[row,col]==valueMax,axis=2)
                count = count >= scale*scale*0.5
                np.place(newDataClass,count,valueMax)
                np.place(classCount,count,scale*scale)
            for classValue in classValues:
                if np.sum(dataClass==classValue) == 0:
                    continue
                count = np.sum(dataClass[row,col]==classValue,axis=2)
                np.place(newDataClass,count > classCount,classValue)
                np.copyto(classCount, count, where= count > classCount)

        if valueMax is not None and classes != []:
            newValueMask = (newDataValue >= valueMin)*(newDataValue <= valueMax)
            np.copyto(newDataValue,newDataClass,where=~newValueMask)
            newData = newDataValue

        if valueMax is not None and classes == []:
            newData = newDataValue

        if valueMax is None and classes != []:
            newData = newDataClass

        if valueMax is None and classes == []:  #bitwise
            newData = np.zeros(shape=newShape,dtype=data.dtype)
            for b in range(int(str(data.dtype).replace('uint','').replace('int',''))):
                mask = np.bitwise_and(np.right_shift(data,b),1)
                count = np.sum(mask[row,col],axis=2)
                mask = count >= (float(np.prod(data.shape))/np.prod(newShape))/2.
                newData = np.bitwise_or(newData,np.left_shift(mask.astype(data.dtype),b))
    return newData

def setBit(data,bit,value,where=None):
    if where is None:
        where = np.ones(data.shape,np.bool)
    newData = np.zeros(data.shape,dtype=data.dtype)
    for b in range(int(str(data.dtype).replace('uint','').replace('int',''))):
        mask = np.bitwise_and(np.right_shift(data,b),1)
        if b == bit:
            if isinstance(where,list):
                mask[where[0],where[1]] = value
            else:
                np.place(mask,where,value)
        newData = np.bitwise_or(newData,np.left_shift(mask.astype(data.dtype),b))
    return newData

def getBit(data,bit):
    mask = np.bitwise_and(np.right_shift(data,bit),1)
    return mask.astype(np.bool)

def getAlphashape(raster,noData, geoTransform, projectionRef):
    log("Calculating alphashape")
    productPoints = []
    productIndices = np.indices(raster.shape).transpose(1,2,0)
    if np.sum(raster!=noData) != 0:
        cLeft, cRight = 0,raster.shape[1]
        for c, col in enumerate(raster.transpose(1,0)):
            colMask = col != noData
            if np.sum(colMask) != 0:
                cLeft = c
                break
        for c, col in enumerate(raster.transpose(1,0)[::-1]):
            colMask = col != noData
            if np.sum(colMask) != 0:
                cRight = colMask.shape[0]-c
                break
        for r,row in enumerate(raster):
            rowMask = row[cLeft:cRight] != noData
            if np.sum(rowMask) == 0:
                continue
            for c,leftPoint in enumerate(rowMask):
                if leftPoint:
                    productPoints.append(productIndices[r,c+cLeft][::-1])
                    break
            for c,rightPoint in enumerate(rowMask[::-1]):
                if rightPoint:
                    productPoints.append(productIndices[r,cRight-1-c][::-1])
                    break
    
    # Exception for nonpolygon boundaries
    if len(productPoints) == 0:
        # Probably not possible
        log('Warning: No point can be deduced to calculate alphashape. Raster is probably all nodata. Using maximum extent.')
        productPoints = [productIndices[0][0],productIndices[0][1],productIndices[1][0],productIndices[1][1]]
    else:
        productAlphashape = alphashape.alphashape(productPoints, 0.)
        if type(productAlphashape) is LineString or type(productAlphashape) is Point:
            log('Warning: Deduced points to calculate alphashape forms a %s, not form a polygon. Using a slightly larger rectangle.' % type(productAlphashape).__name__)
            # xmin ymin xmax ymax
            productPoints = [min(list(zip(*productPoints))[0]),min(list(zip(*productPoints))[1]),max(list(zip(*productPoints))[0]),max(list(zip(*productPoints))[1])]
            # set offset if on the border
            productPoints[0] = 1 if productPoints[0] == 0 else productPoints[0]
            productPoints[1] = 1 if productPoints[1] == 0 else productPoints[1]
            productPoints[2] = productIndices[-1][-1][0]-1 if productPoints[2] == productIndices[-1][-1][0] else productPoints[2]
            productPoints[3] = productIndices[-1][-1][1]-1 if productPoints[3] == productIndices[-1][-1][1] else productPoints[3]
            # make it 1 pixel larger
            productPoints = [(productPoints[0]-1,productPoints[1]-1),(productPoints[0]-1,productPoints[3]+1),(productPoints[2]+1,productPoints[1]-1),(productPoints[2]+1,productPoints[3]+1)]
            productAlphashape = alphashape.alphashape(productPoints, 0.)
        productPoints = productAlphashape.exterior.coords[:]
        
    source = osr.SpatialReference()
    source.ImportFromWkt(projectionRef)
    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)
    transform = osr.CoordinateTransformation(source,target)
    for p,productPoint in enumerate(productPoints):
        productPoint = [productPoint[0]+0.5,productPoint[1]+0.5]
        productPoint = (geoTransform[0] + productPoint[0]*geoTransform[1],geoTransform[3] + productPoint[1]*geoTransform[5])
        productPoint = transform.TransformPoint(productPoint[0],productPoint[1])
        productPoints[p] = [productPoint[1],productPoint[0]]
    productPoints = Polygon(productPoints)
    return str(productPoints)

def getGeoBounds(raster,geoTransform,projectionRef):
    source = osr.SpatialReference()
    source.ImportFromWkt(projectionRef)
    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)
    transform = osr.CoordinateTransformation(source,target)
    lats = []
    lons = []
    for point in [[0,0],[0,raster.shape[1]],[raster.shape[0],0],[raster.shape[0],raster.shape[1]]]:
        point = (geoTransform[0] + point[0]*geoTransform[1],geoTransform[3] + point[1]*geoTransform[5])
        point = transform.TransformPoint(point[0],point[1])
        lats.append(point[0])
        lons.append(point[1])
    bounds = [min(lons),max(lons),min(lats),max(lats)]
    return bounds

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('parameters_file',help='Path to the configuration parameters file')
sysargv = vars(parser.parse_args())
if sysargv['parameters_file'] is not None:
    pFile = open(sysargv['parameters_file'],'r')
    parameters = yaml.load(pFile)
    pFile.close()
    fscDir = parameters['fsc_dir']
    wdsDir = parameters['wds_dir']
    swsDir = parameters['sws_dir']
    gfDir = parameters['gfsc_dir']
    gf1Dir = os.path.join(parameters['output_dir'],'data')
    auxDir = parameters['aux_dir']
    demFile = os.path.join(auxDir,parameters['dem_file'])
    fuwFile = os.path.join(auxDir,parameters['fuw_mask_file'])
    nmFile = os.path.join(auxDir,parameters['nm_mask_file'])
    logDir = os.path.join(parameters['output_dir'],'logs')
else:
    fscDir = None
    wdsDir = None
    swsDir = None
    gfDir = None
    gf1Dir = None
    auxDir = None
    demFile = None
    fuwFile = None
    nmFile = None
    logDir = None
