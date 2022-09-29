#!/usr/bin/python3

import os, sys, argparse, datetime, xmltodict, yaml
import numpy as np

def main():
    def deliver(sysargv,parameters,result):
        # Dump some info
        sysargv['parameters'] = parameters
        sysargv['return_code'] = result
        if 'output_dir' in sysargv:
            if not os.path.exists(sysargv['output_dir']):
                os.makedirs(sysargv['output_dir'])
            f = open(os.path.join(sysargv['output_dir'],'output_info.yaml'),'w')
            f.write(yaml.dump(sysargv))
            f.close()
        return result
    
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('parameters_file',help='Path to the configuration parameters file')
    sysargv = vars(parser.parse_args())
    if sysargv['parameters_file'] is None:
        print('Parameters file not given. Exiting.')
        return deliver(sysargv,{},120)
    pFile = open(sysargv['parameters_file'],'r')
    parameters = yaml.load(pFile)
    pFile.close()
    for parameter in ['aggregation_timespan','product_title',
                        'obsolete_product_id_list', 'fsc_id_list','wds_id_list','sws_id_list','gfsc_id_list',
                        'output_dir','aux_dir','tmp_dir', 'work_dir','fuw_mask_file','nm_mask_file','dem_file',
                        'gfsc_dir','fsc_dir','sws_dir','wds_dir']:
        if parameter not in parameters:
            print('Parameters file incomplete. Exiting.')
            return deliver(sysargv,parameters,121)
    sysargv['day_delta'] = parameters['aggregation_timespan']
    sysargv['output_dir'] = os.path.join(parameters['output_dir'],'data')
    sysargv['product_title'] = parameters['product_title']
    sysargv['tile_id'] = parameters['tile_id']
    sysargv['tmp_dir'] = parameters['tmp_dir']
    sysargv['fsc'] = parameters['fsc_id_list']
    sysargv['wds_sws'] = parameters['wds_id_list'] + parameters['sws_id_list']
    sysargv['gf'] = parameters['gfsc_id_list']
    sysargv['obsolete'] = parameters['obsolete_product_id_list']
    for product in sysargv['obsolete']:
        if product in sysargv['fsc']:
            sysargv['fsc'].remove(product)
        if product in sysargv['wds_sws']:
            sysargv['wds_sws'].remove(product)
    import gf1, gf2, gfio
    # print("GF",sysargv)

    if sysargv['wds_sws'] + sysargv['fsc'] + sysargv['gf'] == []:
        gfio.log('No input products to process.')
        return deliver(sysargv,parameters,31)

    missions = 'S1-S2'
    processingBaseline = 'V101'
    sysargv['product_title'] = sysargv['product_title'].replace('processingBaseline',processingBaseline)
    sysargv['product_title'] = sysargv['product_title'].replace('missions',missions)
    tileId = sysargv['tile_id']

    # Group daily wds and fsc for gf1
    productList = {}    # day:[productTitles]
    for productTitle in sysargv['wds_sws'] + sysargv['fsc']:
        productTimeStamp = datetime.datetime.strptime(productTitle.split('_')[1].split('-')[0],"%Y%m%dT%H%M%S")
        productDay = datetime.date(productTimeStamp.year,productTimeStamp.month,productTimeStamp.day)
        if productDay not in productList:
            productList.update({productDay:[]})
        productList[productDay].append(productTitle)

    # Get all product metadata and decide on the output
    productDirs = []
    productTypes = []
    productTitles = []
    productTimeStamps = []
    productXmls = []
    productStartDates = []
    productEndDates = []
    productInputTitles = []
    productInputTypes = []
    productInputTimeStamps = []

    # add gf1 products planned
    for productDay in productList:
        productInputTitle = []
        productInputType = []
        productInputTimeStamp = []

        for productTitle in productList[productDay]:
            try:
                productTimeStamp = datetime.datetime.strptime(productTitle.split('_')[1].split('-')[0],"%Y%m%dT%H%M%S")
                productInputTitle.append(productTitle)
                productInputType.append(productTitle.split('_')[0])
                productInputTimeStamp.append(productTimeStamp)
            except Exception as e:
                gfio.log("Problem reading product list")
                gfio.log(e)
                return deliver(sysargv,parameters,32)

        if productInputTitle == []:
            gfio.log("Empty product list")
            return deliver(sysargv,parameters,33)

        if 'WDS' not in productInputType and 'SWS' not in productInputType:
            continue

        productTimeStamp = datetime.datetime(productDay.year,productDay.month,productDay.day)
        productTitle = '_'.join(['GFSC1',productTimeStamp.strftime("%Y%m%d"),missions,tileId,processingBaseline])
        productDir = gfio.getDirPath(productTitle)

        productDirs.append(productDir)
        productTypes.append('GFSC1')
        productTitles.append(productTitle)
        productTimeStamps.append(productTimeStamp)
        productXmls.append(None)
        productStartDates.append(None)
        productEndDates.append(None)
        productInputTitles.append(productInputTitle)
        productInputTypes.append(productInputType)
        productInputTimeStamps.append(productInputTimeStamp)

    for productTitle in sysargv['gf'] + sysargv['wds_sws'] + sysargv['fsc']:
        try:
            productType = gfio.getProductType(productTitle)
            productDir = gfio.getDirPath(productTitle)
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
            if productType in ['FSC','WDS','SWS']:
                productTimeStamp = datetime.datetime.strptime(productTitle.split('_')[1].split('-')[0],"%Y%m%dT%H%M%S")
                productInputTitles.append([productTitle])
                productInputTypes.append([productType])
                productInputTimeStamps.append([productTimeStamp])
            if productType == 'GFSC':
                productTimeStamp = datetime.datetime.strptime(productTitle.split('_')[1].split('-')[0],"%Y%m%d")
                productInputXmls = productXml['gmd:MD_Metadata']['gmd:series']['gmd:DS_OtherAggregate']['gmd:seriesMetadata']
                if len(productInputXmls) == 1:
                    productInputXmls = [productInputXmls]
                for productInputXml in productInputXmls:
                    productInputTitle = []
                    productInputType = []
                    productInputTimeStamp = []
                    productInputTimeStamp.append(datetime.datetime.strptime(productInputXml['gmd:MD_Metadata']['gmd:fileIdentifier']['gco:CharacterString'].split('_')[1].split('-')[0],"%Y%m%dT%H%M%S"))
                    productInputTitle.append(productInputXml['gmd:MD_Metadata']['gmd:fileIdentifier']['gco:CharacterString'])
                    productInputType.append(productInputXml['gmd:MD_Metadata']['gmd:fileIdentifier']['gco:CharacterString'].split('_')[0])
                productInputTitles.append(productInputTitle)
                productInputTypes.append(productInputType)
                productInputTimeStamps.append(productInputTimeStamp)
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
            return deliver(sysargv,parameters,32)

    if productDirs == []:
        gfio.log("Empty product list")
        return deliver(sysargv,parameters,33)

    result = 0
    productOrder = (np.argsort(productTimeStamps)[::-1]).tolist()
    finalProductTimeStamp = productTimeStamps[productOrder[0]]
    finalProductTimeStamp = datetime.datetime(finalProductTimeStamp.year,finalProductTimeStamp.month,finalProductTimeStamp.day,23,59,59)

    #produce GF1s first
    for p in productOrder:
        if productTypes[p] != 'GFSC1':
            continue
        args = []
        for r,productInputTitle in enumerate(productInputTitles[p]):
            if productInputTypes[p][r] in ['WDS','SWS']:
                args += ["-w",productInputTitle]
            if productInputTypes[p][r] == 'FSC':
                args += ["-f",productInputTitle]
        args += ["-o",sysargv['output_dir']]
        args += ["-p",productTitles[p]]
        args += ["-t",sysargv['tmp_dir']]
        sys.argv[1:] = args
        result = gf1.main()
        if result != 0:
            return deliver(sysargv,parameters,result)

    #produce GF2
    if result == 0:
        args = []
        for p in productOrder:
            if productTypes[p] == 'GFSC':
                args += ["-g",productTitles[p]]
            if productTypes[p] == 'GFSC1':
                args += ["-g1",productTitles[p]]
            if productTypes[p] == 'FSC':
                args += ["-f",productTitles[p]]
            args += ["-o",sysargv['output_dir']]
            args += ["-p",sysargv['product_title']]
            args += ["-t",sysargv['tmp_dir']]
        sys.argv[1:] = args
        result = gf2.main()

    return deliver(sysargv,parameters,result)

if __name__ == "__main__":
    sys.exit(main())
