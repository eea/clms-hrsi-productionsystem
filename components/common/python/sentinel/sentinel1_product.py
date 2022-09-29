from .sentinel1_id import Sentinel1Id

class Sentinel1Product(object):
    '''
    Contains the different components of a Sentinel-1 product metadata.

    :param product_id: S1 product ID
    :param sentinel1_id: Sentinel1Id object, containing fileds parsed from S1 product ID
    :param product_path: S1 product path on DIAS
    :param measurement_date: the measurement date of the product (from its product id)
    :param dias_publication_date: actual date of publication in the DIAS catalogue
    :param other_metadata: a named tupled base on other fields parsed from the DIAS catalogue
    :param manifest: a named tupled base on other fields parsed from the manifest.safe
    '''

    def __init__(self, product_path, publication_date, other_metadata, manifest=None):
        '''
        Constructor from a Sentinel-1 product retrieve the DIAS.

        :param product_path: e.g. /eodata/.../S1A_IW_GRDH_1SDV_20210414T051842_20210414T051907_037442_0469EF_804E.SAFE
        :param publication_date: (datetime) publication date from DIAS metadata
        '''
        # Extract information from the product filename
        self.sentinel1_id = Sentinel1Id(product_path)

        self.product_id = self.sentinel1_id.full_id
        self.product_path = product_path
        self.measurement_date = self.sentinel1_id.start_time

        # There is also the 'update' param that we don't use.
        self.dias_publication_date = publication_date

        # This object contain other metadata parsed from the DIAS catalogue
        # nominal member list: 'orbitNumber',
            #  'orbitDirection',
            #  'relativeOrbitNumber',
            #  'missionTakeId',
            #  'timeliness',
            #  'gmlgeometry',
            #  'thumbnail',
            #  'productIdentifier'
        self.other_metadata = other_metadata


        # This object contain other metadata parsed from the manifest.safe
        # nominal member list: 'sliceNumber',
        #                      'totalSlices'
        if manifest is not None:
            self.manifest = manifest
