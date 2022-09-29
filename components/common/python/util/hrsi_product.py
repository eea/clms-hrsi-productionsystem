from os.path import basename

class HrsiProduct(object):
    '''
    Contains the different components of a Sentinel-2 product metadata.

    :param product_id: HR-S&I product ID
    :param product_path: HR-S&I product path on DIAS
    :param product_type: HR-S&I product type
    :param tile_id: S2 tile identifier
    :param cloud_cover: the product cloud cover
    :param snow_cover: the product snow cover
    :param creation_date: the creation date of the product (based on ESA processing)
    :param publication_date: actual date of publication in the DIAS catalogue
    :param mission_type: (string) % mission type ('S1'|'S2'|'S1-S2')
    :param gml_geometry: (string) % geometry as gml format
    '''

    def __init__(self,
                product_path,
                product_type,
                cloud_cover,
                snow_cover,
                measurement_date,
                creation_date,
                publication_date,
                mission_type=None,
                gml_geometry=None):
        '''
        Constructor from a HR-S&I product retrieve the DIAS.

        :param product_path: e.g. /eodata/.../FSC_20210415T091255_S2A_T36VVQ_V100_1
        :param publication_date: (datetime) publication date from metadata
        :param cloud_cover: (float) % cloud cover from metadata
        :param snow_cover: (float) % snow cover from metadata
        :param mission_type: (string) % mission type ('S1'|'S2'|'S1-S2')
        :param gml_geometry: (string) % geometry as gml format
        '''
        # Extract information from the product metadata
        self.product_type = product_type
        self.product_id = basename(product_path)
        self.tile_id = self.product_id.split('_')[3]
        self.product_path = product_path
        self.cloud_cover = cloud_cover
        self.snow_cover = snow_cover
        self.measurement_date = measurement_date
        self.creation_date = creation_date
        self.publication_date = publication_date
        self.mission_type = mission_type
        self.gml_geometry = gml_geometry