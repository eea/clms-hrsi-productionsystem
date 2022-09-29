#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tool to create the DTM files expected by MAJA

Author:         Olivier Hagolle <olivier.hagolle@cnes.fr>
Project:        StartMaja, CNES

==================== Copyright
Software (tuilage_mnt_eau_S2.py)

Copyright© 2018 Centre National d’Etudes Spatiales

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License version 3
as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this program.  If not, see
https://www.gnu.org/licenses/gpl-3.0.fr.html
"""
import sys, os
assert sys.version_info >= (3,5)
from aux_data_creation.make_s2_tiled_eu_dem_maja_inputs import lib_mnt


class TuilageParamsConverter(object):
    """
    Class to create the class storing the tile- and path-parameters
    """
    def getSiteFromParams(self, name, proj, EPSG, pasX, pasY, origX, origY):
        """
        Create the tile-parameters internally using variables
        """
        tx_min, ty_min, tx_max, ty_max, marge = 0,0,0,0,0
        return lib_mnt.classe_site(name, proj, EPSG, "EPSG:"+str(EPSG),
                                       tx_min, ty_min, tx_max, ty_max,
                                       pasX, pasY, marge, origX, origY)
        
    def getSiteFromFile(self, filename):
        """
        Create the TileParameters reading a file
        """
        return lib_mnt.lire_fichier_site(filename)
    
    def getPathsFromFile(self, filename):
        """
        Create the Path Parameters reading a file
        """        
        return lib_mnt.lire_param_txt(filename)

class TuilageSentinel(object):
    """
    Reprojette et decoupe un mnt SRTM sur les tuiles d'un site 
    Les paramètres sont dans parametres.py, dont le nom du site qui sert à déterminer le fichier de paramètres du tuilage d'un site (ex pyrenees.py)
    
    """    
    def unzip_water(self, dirInWater, filenames, dirOut):
        """
        Unzip Water-SWBD files
        """
        import zipfile
        import re
        files = []
        for pattern in filenames:
            files += list(os.path.join(dirInWater, f) for f in os.listdir(dirInWater) if re.search(pattern + ".*?\.zip", f))
        if(len(files) == 0):
            raise OSError("Cannot find SWBD zip files!")
        for fn in files:
            print("Unzipping {0}".format(fn))
            zip_ref = zipfile.ZipFile(fn, 'r')
            zip_ref.extractall(dirOut)
            zip_ref.close()
        return 0
    
    def run(self, dirInSRTM, dirInWater, dirOut, dirOutWater, resolution, site, mnt, waterOnly=False, wdir=None, water_zipped=False, example_l1c_file=None):
        from math import ceil, floor
        from osgeo import osr
        import tempfile

        if wdir is None:
            working_dir = tempfile.mkdtemp()
        else:
            working_dir = tempfile.mkdtemp(dir=wdir)

        os.environ['LC_NUMERIC'] = 'C'
        # lecture du fichier de paramètres et du fichier site
        rep_mnt_in, rep_mnt, _, _ = dirInSRTM, dirOut, dirInWater, dirOutWater
        print(rep_mnt_in, rep_mnt, dirInWater, dirOutWater)
        
        # ==========création de la liste des fichiers planet_observer
        # conversion des coordonnées des coins en lat_lon
        latlon = osr.SpatialReference()
        latlon.SetWellKnownGeogCS("WGS84")
        proj_site = osr.SpatialReference()
        proj_site.ImportFromEPSG(site.EPSG_out)
        transform = osr.CoordinateTransformation(proj_site, latlon)
        
        # recherche des 4 coins du site
        ulx_site = site.orig_x + site.tx_min * site.pas_x  # upper left
        uly_site = site.orig_y + site.ty_max * site.pas_y
        lrx_site = site.orig_x + (site.tx_max + 1) * site.pas_x + site.marge  # lower left
        lry_site = site.orig_y + (site.ty_min - 1) * site.pas_y - site.marge
        
        ul_latlon = transform.TransformPoint(ulx_site, uly_site, 0)
        lr_latlon = transform.TransformPoint(lrx_site, lry_site, 0)
        
        liste_fic_mnt = []
        
        ############# MNT SRTM du CGIAR
        if mnt == "SRTM":
            original_res = 90
            # liste des fichiers SRTM nécessaires
            if (ul_latlon[1]) > 60 or (lr_latlon[1] > 60):
                print("#################################################")
                print("latitude supérieure à 60 degrés, pas de donnees SRTM")
                print("#################################################")
                sys.exit(-3)
        
            ul_latlon_srtm = [int(int(ul_latlon[0] + 180) / 5 + 1), int(int(60 - ul_latlon[1]) / 5 + 1)]
            lr_latlon_srtm = [int(int(lr_latlon[0] + 180) / 5 + 1), int(int(60 - lr_latlon[1]) / 5 + 1)]
            print(ul_latlon_srtm)
            print(lr_latlon_srtm)
            for x in range(ul_latlon_srtm[0], lr_latlon_srtm[0] + 1):
                for y in range(ul_latlon_srtm[1], lr_latlon_srtm[1] + 1):
                    liste_fic_mnt.append("srtm_%02d_%02d.tif" % (x, y))
        
            print(ul_latlon, lr_latlon)
            print(ul_latlon_srtm, lr_latlon_srtm)
            print(liste_fic_mnt)
        
        ########## MNT Planet Observer
        elif mnt == "PO":
            original_res = 90
            ul_latlon_po = [int(floor(ul_latlon[0])), int(floor(ul_latlon[1]))]
            lr_latlon_po = [int(floor(lr_latlon[0])), int(floor(lr_latlon[1]))]
        
            for x in range(ul_latlon_po[0], lr_latlon_po[0] + 1):
                for y in range(lr_latlon_po[1], ul_latlon_po[1] + 1):
                    if x >= 0:
                        ew = "e"
                        num_x = x
                    else:
                        ew = "w"
                        num_x = -x
                    if y >= 0:
                        ns = "n"
                        num_y = y
                    else:
                        ns = "s"
                        num_y = -y
                    liste_fic_mnt.append("%s%03d/%s%02d.dt1" % (ew, num_x, ns, num_y))
        
            print(ul_latlon, lr_latlon)
            print(ul_latlon_po, lr_latlon_po)
            print(liste_fic_mnt)
        
        ########## MNT EUDEM
        elif mnt == "EUDEM":
            original_res = 10
            if example_l1c_file is None:
                raise Exception('example_l1c_file must be filled for EUDEM processing')
            from si_geometry.geometry_functions import RasterPerimeter
            s2_tile_raster_perimeter = RasterPerimeter(example_l1c_file)
            liste_fic_mnt = []
            for el in os.listdir(dirInSRTM):
                if not el.split('.')[-1].lower() == 'tif':
                    continue
                dem_raster_perimeter = RasterPerimeter('%s/%s'%(dirInSRTM, el))
                if not s2_tile_raster_perimeter.intersects(RasterPerimeter('%s/%s'%(dirInSRTM, el))):
                    continue
                print('%s: OK'%el)
                liste_fic_mnt.append(el)
            print(liste_fic_mnt)
            
        if len(liste_fic_mnt) == 0:
            return 'no_intersection_with_src_dem'
            
        
        # liste des fichiers SWBD nécessaires
        ul_latlon_swbd = [int(floor(ul_latlon[0])), int(floor(ul_latlon[1]))]
        lr_latlon_swbd = [int(floor(lr_latlon[0])), int(floor(lr_latlon[1]))]
        print(ul_latlon, lr_latlon)
        print(ul_latlon_swbd, lr_latlon_swbd)

        calcul_masque_eau_mnt = 0
        dico_water_shapes = dict()
        if dirInWater is None:
            dico_water_shapes = None
        elif os.path.isdir(dirInWater):
            dico_water_shapes['mode'] == 'SWBD'
            if (ul_latlon[1]) > 60 or (lr_latlon[1] > 60):
                print("#################################################")
                print("latitude supérieure à 60 degrés, pas de donnees SRTM")
                print("le masque d'eau est généré à partir du MNT")
                print("#################################################")
                calcul_masque_eau_mnt = 1
            
            dico_water_shapes['rep_swbd'] = dirInWater
            dico_water_shapes['liste_fic_eau'] = []
            dico_water_shapes['liste_centre_eau'] = []
            for x in range(ul_latlon_swbd[0], lr_latlon_swbd[0] + 1):
                for y in range(lr_latlon_swbd[1], ul_latlon_swbd[1] + 1):
                    if x >= 0:
                        ew = "e"
                        num_x = x
                    else:
                        ew = "w"
                        num_x = -x
                    if y >= 0:
                        ns = "n"
                        num_y = y
                    else:
                        ns = "s"
                        num_y = -y
            
                    dico_water_shapes['liste_fic_eau'].append("%s%03d%s%02d" % (ew, num_x, ns, num_y))
                    dico_water_shapes['liste_centre_eau'].append([x + 0.5, y + 0.5])
            print("longitudes", ul_latlon_swbd[0], lr_latlon_swbd[0])
            print("latitudes", lr_latlon_swbd[1], ul_latlon_swbd[1])
            print("center coordinates", dico_water_shapes['liste_centre_eau'])
            print(dico_water_shapes['liste_fic_eau'])
        elif os.path.isfile(dirInWater):
            dico_water_shapes['mode'] = 'simple_shapefile'
            dico_water_shapes['shapefile'] = dirInWater

                



        # Fusion des mnt_srtm en un seul
        fic_mnt_in = lib_mnt.fusion_mnt(liste_fic_mnt, rep_mnt_in, site.nom, working_dir)
        print("############", fic_mnt_in)
        
        ####################Boucle de création des fichiers MNT et eau pour chaque tuile
        
        for tx in range(site.tx_min, site.tx_max + 1):
            for ty in range(site.ty_min, site.ty_max + 1):

                ulx = site.orig_x + tx * site.pas_x  # upper left
                uly = site.orig_y + ty * site.pas_y
                lrx = site.orig_x + (tx + 1) * site.pas_x + site.marge  # lower left
                lry = site.orig_y + (ty - 1) * site.pas_y - site.marge
        
                lrx_original_res = int(ceil((lrx - ulx) / float(original_res))) * original_res + ulx
                lry_original_res = uly - int(ceil((uly - lry) / float(original_res))) * original_res
        
                lrx_coarse = int(ceil((lrx - ulx) / float(resolution))) * resolution + ulx
                lry_coarse = uly - int(ceil((uly - lry) / float(resolution))) * resolution
        
                nom_tuile = site.nom
        
                print("nom de la tuile", nom_tuile, tx, ty)
                ###pour le MNT
                rep_mnt_out = working_dir
                print("MNT Reps: {0} {1} {2}".format(rep_mnt, rep_mnt_in, rep_mnt_out))

                print("############### c'est parti")                     
                # Basse résolution
                mnt_coarse = lib_mnt.classe_mnt(fic_mnt_in, rep_mnt_out, nom_tuile, ulx, uly, lrx_coarse, lry_coarse, resolution, site.chaine_proj)
                mnt_coarse.decoupe()
                if not waterOnly:
                    
                    # calcul du gradient à la res originale
                    mnt_original_res = lib_mnt.classe_mnt(fic_mnt_in, rep_mnt_out, nom_tuile, ulx, uly, lrx_original_res, lry_original_res, original_res, site.chaine_proj)
                    mnt_original_res.decoupe()
                    (fic_dz_dl_srtm, fic_dz_dc_srtm) = mnt_original_res.calcul_gradient()
                        
                    mnt_coarse.reech_gradient(fic_dz_dl_srtm, fic_dz_dc_srtm)
                    mnt_coarse.calcul_pente_aspect_fic()
        
                    # Haute résolution 10 et 20m
                    for full_res in [10, 20]:
                        mnt_full = lib_mnt.classe_mnt(fic_mnt_in, rep_mnt_out, nom_tuile, ulx, uly, lrx, lry, full_res, site.chaine_proj)
                        mnt_full.decoupe()
                        mnt_full.reech_gradient(fic_dz_dl_srtm, fic_dz_dc_srtm)
                        mnt_full.calcul_pente_aspect_fic()
                        
                    #create a DEM file that preserves NAN from input with nearest interpolation
                    fic_dem_with_nan0 = '%s_dem_20m_0.tif'%rep_mnt_out
                    print('fic_dem_with_nan: %s'%(os.path.abspath(fic_dem_with_nan)))
                    chaine_etendue = str(ulx) + ' ' + str(lry) + ' ' + str(lrx) + ' ' + str(uly)
                    commande = 'gdalwarp -overwrite -r near -ot Int16 -dstnodata -32768 -of GTIFF -tr %d %d -te %s -t_srs %s %s %s'%(20, 20, chaine_etendue, \
                        site.chaine_proj, fic_mnt_in, fic_dem_with_nan0)
                    print(commande)
                    os.system(commande)
                    
                    #create a DEM file that preserves NAN from input with cubic interpolation (does not exactly preserve NAN, this is why we have to apply NAN mask afterwards)
                    fic_dem_with_nan = '%s_dem_20m.tif'%rep_mnt_out
                    print('fic_dem_with_nan: %s'%(os.path.abspath(fic_dem_with_nan)))
                    chaine_etendue = str(ulx) + ' ' + str(lry) + ' ' + str(lrx) + ' ' + str(uly)
                    commande = 'gdalwarp -overwrite -r cubic -ot Int16 -dstnodata -32768 -of GTIFF -tr %d %d -te %s -t_srs %s %s %s'%(20, 20, chaine_etendue, \
                        site.chaine_proj, fic_mnt_in, fic_dem_with_nan)
                    print(commande)
                    os.system(commande)
    
                    #read nearest interpolation NAN mask
                    ds = gdal.Open(fic_dem_with_nan0)
                    demdata0 = np.ma.masked_invalid(ds.GetRasterBand(1).ReadAsArray())
                    no_data_value_loc = ds.GetRasterBand(1).GetNoDataValue()
                    demdata0.mask[demdata0==no_data_value_loc] = True
                    ds = None
                    del ds
                    #delete nearest interpolated file
                    os.unlink(fic_dem_with_nan0)
                    
                    #apply nearest interpolation NAN mask to cubic interpolated file
                    ds = gdal.Open(fic_dem_with_nan, 1)
                    band = ds.GetRasterBand(1)
                    no_data_value_loc = band.GetNoDataValue()
                    input_data_loc = band.ReadAsArray()
                    input_data_loc[demdata0.mask] = no_data_value_loc
                    band.WriteArray(input_data_loc)
                    ds = None
                    del ds

                
                ### Pour l'eau        
                if calcul_masque_eau_mnt == 0:
                    mnt_coarse.decoupe_eau(dico_water_shapes)
                else:
                    mnt_coarse.calcul_masque_mnt(rep_mnt_out, nom_tuile)
        return 'valid'
                    


import optparse
###########################################################################
class OptionParser(optparse.OptionParser):

    def check_required(self, opt):
        option = self.get_option(opt)

        # Assumes the option's 'default' is set to None!
        if getattr(self.values, option.dest) is None:
            self.error("%s option not supplied" % option)
 
if __name__ == "__main__":
    if len(sys.argv) == 1:
        prog = os.path.basename(sys.argv[0])
        print('      ' + sys.argv[0] + ' [options]')
        print("     Aide : ", prog, " --help")
        print("        ou : ", prog, " -h")
        print("example : python %s -p parametres_srtm.txt -s 32SNE.txt -m SRTM -c 240" % sys.argv[0])
        sys.exit(-1)
    else:
        usage = "usage: %prog [options] "
        parser = OptionParser(usage=usage)
        parser.set_defaults(eau_seulement=False)
        parser.set_defaults(sans_numero=False)
    
        parser.add_option("-p", "--parametre", dest="fic_param", action="store", type="string", \
                          help="fichier de parametre", default=None)
        parser.add_option("-s", "--site", dest="fic_site", action="store", type="string", \
                          help="fichier de description du site, or granule ID if --kml is used", default=None)
        parser.add_option("-k", "--kml", action="store", type="string", \
                          help="S2 Tiling grid KML file", default=None)
        parser.add_option("-m", "--mnt", dest="mnt", action="store", type="choice", \
                          help="SRTM, PO (Planet Observer) ou EUDEM", choices=['SRTM', 'PO', 'EUDEM'], default=None)
        parser.add_option("-c", dest="COARSE_RES", action="store", type="int", \
                          help="Coarse resolution", default=240)
        parser.add_option("-e", dest="eau_seulement", action="store_true", \
                          help="Traitement des masques d'eau seulement")
        parser.add_option("-n", dest="sans_numero", action="store_true", \
                          help="Traitement sans numero de tuile")
    
        (options, args) = parser.parse_args()
        parser.check_required("-p")
        parser.check_required("-s")
        parser.check_required("-m")
        converter = TuilageParamsConverter()
        dirInSRTM, dirOut, dirInWater, dirOutWater = converter.getPathsFromFile(options.fic_param)
        if options.kml:
            site = lib_mnt.lire_fichier_site_kml(options.kml, options.fic_site)
        else:
            site = converter.getSiteFromFile(options.fic_site)
        mntcreator = TuilageSentinel()
        mntcreator.run(dirInSRTM, dirInWater, dirOut, dirOutWater, options.COARSE_RES, site=site, mnt=options.mnt, waterOnly=options.eau_seulement, water_zipped=False)
