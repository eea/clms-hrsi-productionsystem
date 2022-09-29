from .sentinel2_l1c_id import Sentinel2L1cId

class Sentinel2Product(object):
    '''
    Contains the different components of a Sentinel-2 product metadata.

    :param product_id: S2 product ID
    :param product_path: S2 product path on DIAS
    :param sentinel2_id: Sentinel2L1cId object, containing fileds parsed from S2 product ID
    :param tile_id: S2 tile identifier
    :param cloud_cover: the product cloud cover (from ESA processing detection)
    :param snow_cover: the product snow cover (from ESA processing detection)
    :param measurement_date: the measurement date of the product (from its product id)
    :param eas_creation_date: the creation date of the product (based on ESA processing)
    :param dias_publication_date: actual date of publication in the DIAS catalogue
    '''

    def __init__(self, product_path, publication_date, cloud_cover, snow_cover):
        '''
        Constructor from a Sentinel-2 product retrieve the DIAS.

        :param product_path: e.g. /eodata/.../S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE
        :param publication_date: (datetime) publication date from DIAS metadata
        :param cloud_cover: (float) % cloud cover from DIAS metadata
        :param snow_cover: (float) % snow cover from DIAS metadata
        '''
        # Extract information from the product filename
        self.sentinel2_id = Sentinel2L1cId(product_path)

        self.tile_id = self.sentinel2_id.tile
        self.product_id = self.sentinel2_id.full_id
        self.product_path = product_path
        self.cloud_cover = cloud_cover
        self.snow_cover = snow_cover
        self.measurement_date = self.sentinel2_id.start_time
        self.esa_creation_date = self.sentinel2_id.discriminator

        # There is also the 'update' param that we don't use.
        self.dias_publication_date = publication_date
