#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, os, shutil, glob, subprocess
assert sys.version_info >= (3,5)
import numpy as np
try:
    from osgeo import gdal
except:
    import gdal
import optparse
from aux_data_creation.make_s2_tiled_eu_dem_maja_inputs import FileSystem


def count_number_zeros_or_negative(input_raster):
    
    ds = gdal.Open(input_raster)
    band = ds.GetRasterBand(1)
    data_ar = np.ma.masked_invalid(band.ReadAsArray())
    no_data_value = band.GetNoDataValue()
    data_ar.mask[data_ar == no_data_value] = True
    ds, band = None, None
    del ds, band

    print('%s :\n  -> %d NANs (%s), %d zeros, %d < 0, min=%d, max=%d'%(input_raster, np.sum(data_ar.mask), no_data_value, np.sum(data_ar==0), np.sum(data_ar<0), np.min(data_ar), np.max(data_ar)))
    
    #values < 1000
    data_ar.mask[:,:] = False
    data_ar = data_ar.flatten()
    values = set([el for el in data_ar.tolist() if el < -1000])
    print(values)
    print('\n')
    
    

class MAJAConverter(object):
    def gdalinfo(self, fic_mnt_in):
        ds=gdal.Open(fic_mnt_in)
        (ulx,resx,dum1,uly,dum2,resy)=ds.GetGeoTransform()
    
        nbCol=ds.RasterXSize
        nbLig=ds.RasterYSize
    
        proj=ds.GetProjectionRef().split('"')[1].split('"')[0]
    
        inband  = ds.GetRasterBand(1)
    
        dtm=inband.ReadAsArray(0, 0, nbCol, nbLig).astype(np.float)
        moyenne=np.mean(dtm)
        ecart=np.std(dtm)
    
        return(proj,ulx,uly,resx,resy,nbCol,nbLig,moyenne,ecart)
    
    ###########################################################################
    
    def writeHDR(self, hdr_out,tuile,proj,ulx,uly,resx,resy,nbCol,nbLig,moyenne,ecart) :
        hdr_template=os.path.dirname(os.path.realpath(__file__)) + "/auxdata/MAJA_HDR_TEMPLATE.HDR"
        #compute epsg_code
        try:
            epsg_asc=proj.split('_')[-1]
            epsg_num=int(epsg_asc[0:-1])
        except:
            epsg_asc=proj.split()[-1]
            epsg_num=int(epsg_asc[0:-1])
    
        if epsg_asc.endswith('N'):
            epsg="326%02d"%epsg_num
        else:
            epsg="327%d02"%epsg_num
        print(epsg)
    
        proj="WGS 84 / UTM zone %s"%epsg_asc
    
        print(proj,epsg)
        
        with open(hdr_out,"w") as fout:
            with open(hdr_template) as fin:
                lignes=fin.readlines()
                for lig in lignes:
                    if lig.find("tuile")>0:
                         lig=lig.replace("tuile","T"+tuile)
                    elif lig.find("epsg")>0:
                         lig=lig.replace("epsg",epsg)
                    elif lig.find("proj")>0:
                         lig=lig.replace("proj",proj)
                    elif lig.find("ulx")>0:
                         lig=lig.replace("ulx",str(int(ulx)))
                    elif lig.find("uly")>0:
                         lig=lig.replace("uly",str(int(uly)))
                    elif lig.find("resx")>0:
                         lig=lig.replace("resx",str(int(resx)))                 
                    elif lig.find("resy")>0:
                         lig=lig.replace("resy",str(int(resy)))  
                    elif lig.find("nbLig")>0:
                         lig=lig.replace("nbLig",str(nbLig))  
                    elif lig.find("nbCol")>0:
                        lig=lig.replace("nbCol",str(nbCol))
                    elif lig.find("meanAlt")>0:
                         lig=lig.replace("meanAlt",str(moyenne))
                    elif lig.find("stdAlt")>0:
                         lig=lig.replace("stdAlt",str(ecart))
                    fout.write(lig)
                
    def run(self, tuile, rep_mnt_in, coarse, out, zone_selection_shapefile=None):
        
        print('\n\nConversion start !')
        
        fic_mnt_in=glob.glob(rep_mnt_in+'/'+'*_10m.mnt')[0]
        FileSystem.createDirectory(out)
        
        # creation of output directory
        rep_mnt_base = "S2__TEST_AUX_REFDE2_T%s_0001"%tuile
        rep_mnt_out=out + "/" + rep_mnt_base
            
        hdr_out=rep_mnt_out+"/"+rep_mnt_base+".HDR"
        dbl_dir_out=rep_mnt_out+"/"+rep_mnt_base+".DBL.DIR"
        os.makedirs(dbl_dir_out, exist_ok=True)
        
        
        # read the parameters of the tile dimension, projection and extent
        (proj,ulx,uly,resx,resy,nbCol,nbLig,moyenne,ecart)=self.gdalinfo(fic_mnt_in)
        
        # write the HDR file
        self.writeHDR(hdr_out,tuile,proj,ulx,uly,resx,resy,nbCol,nbLig,moyenne,ecart)
        

        # now prepare binary file
        resolutions=[10,20,coarse]

        
        # Altitude 10m, 20m, 240m
        suff_proto="mnt"
        suff_MAJA=["ALT_R1", "ALT_R2", "ALC"]
        base_in=fic_mnt_in
        rac_out=dbl_dir_out+'/'+rep_mnt_base
        for i,res in enumerate(resolutions):
            nom_in=base_in.replace("_10m.mnt","_%sm.%s"%(res,suff_proto))
            nom_out=rac_out+"_%s.TIF"%suff_MAJA[i]                           
            commande="gdal_translate -of GTIFF %s %s"%(nom_in,nom_out)
            print(commande)
            os.system(commande)

        
        # Slope 10m, 20m, 240m SLP_R1, SLP_R2, SLC
        suff_proto="slope"
        suff_MAJA=["SLP_R1", "SLP_R2", "SLC"]
        base_in=fic_mnt_in
        rac_out=dbl_dir_out+'/'+rep_mnt_base
        for i,res in enumerate(resolutions):
            nom_in=base_in.replace("_10m.mnt","_%sm.%s"%(res,suff_proto))
            nom_out=rac_out+"_%s.TIF"%suff_MAJA[i]                           
            commande="gdal_translate -of GTIFF %s %s"%(nom_in,nom_out)
            print(commande)
            os.system(commande)
        
        
        # Aspect 10m, 20m ASP_R1, ASP_R2, ASC
        suff_proto="aspect"
        suff_MAJA=["ASP_R1", "ASP_R2", "ASC"]
        base_in=fic_mnt_in
        rac_out=dbl_dir_out+'/'+rep_mnt_base
        for i,res in enumerate(resolutions):
            nom_in=base_in.replace("_10m.mnt","_%sm.%s"%(res,suff_proto))
            nom_out=rac_out+"_%s.TIF"%suff_MAJA[i]                           
            commande="gdal_translate -of GTIFF %s %s"%(nom_in,nom_out)
            print(commande)
            os.system(commande)
        
        # Water Mask
        suff_proto="eau"
        suff_MAJA="MSK"
        base_in=fic_mnt_in
        rac_out=dbl_dir_out+'/'+rep_mnt_base
        res=coarse
        nom_in=base_in.replace("_10m.mnt","_%sm.%s"%(res,suff_proto))
        nom_out=rac_out+"_%s.TIF"%suff_MAJA                           
        commande="gdal_translate -of GTIFF  %s %s"%(nom_in,nom_out)
        print(commande)
        os.system(commande)
        
        #dem mask
        dem_20m_file_in = glob.glob(rep_mnt_in+'/'+'*dem_20m.tif')[0]
        dem_20m_file = os.path.join(dbl_dir_out, 'dem_20m.tif')
        shutil.move(dem_20m_file_in, dem_20m_file)
        
        if zone_selection_shapefile is not None:
            from si_geometry.geometry_functions import RasterPerimeter
            shape_outside = '.'.join(fic_mnt_in.split('.')[0:-1]) + '_zone_outside_selection.shp'
            s2_tile_raster_perimeter = RasterPerimeter(dem_20m_file)
            exclusion_zone_exists = s2_tile_raster_perimeter.clip_multipolygon_shapefile_to_raster_perimeter(zone_selection_shapefile, shape_outside, outside=True)
            if exclusion_zone_exists:
                value_burn = 2
                commande = "gdal_rasterize -burn %s -l %s %s %s"%(value_burn, os.path.basename(shape_outside)[0:-4], shape_outside, dem_20m_file)
                print(commande)
                os.system(commande)




###########################################################################
class OptionParser (optparse.OptionParser):
 
    def check_required (self, opt):
      option = self.get_option(opt)
 
      # Assumes the option's 'default' is set to None!
      if getattr(self.values, option.dest) is None:
          self.error("%s option not supplied" % option)

###########################################################################

if __name__ == "__main__":
    if len(sys.argv)==1  :
        prog = os.path.basename(sys.argv[0])
        print('      '+sys.argv[0]+' [options]')
        print("       Help : ", prog, " --help")
        print("       Or : ", prog, " -h")
        print("example : python %s -t 34LGJ -f mnt/34LGJ"%sys.argv[0])
        sys.exit(-1)
    else:
        usage = "usage: %prog [options] "
        parser = OptionParser(usage=usage)
        parser.set_defaults(eau_seulement=False)
        parser.set_defaults(sans_numero=False)
        
        parser.add_option("-t", "--tile", dest="tile", action="store", type="string", \
                          help="tile name",default=None)
        parser.add_option("-f", "--folder", dest="folder", action="store", type="string", \
                          help="folder where the input DTM is located", default=None)
        parser.add_option("-o", "--out", dest="out", action="store", type="string", \
                          help="folder where the MAJA-DTM shall be written to", default=os.path.dirname(os.path.realpath(__file__)))
        parser.add_option("-c", dest="coarse_res", action="store", type="int",  \
                          help="Coarse resolution", default=240)	
        
        (options, args) = parser.parse_args()
        parser.check_required("-t")
        parser.check_required("-f")
        converter = MAJAConverter()
        converter.run(options.tile, options.folder, options.coarse_res, options.out)
