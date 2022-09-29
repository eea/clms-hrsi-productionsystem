import time
import urllib3
import json
import itertools
from functools import partial
from collections import namedtuple
from xml.etree import ElementTree
from requests import exceptions

from .rest_util import RestUtil
from .request_util import RequestUtil
from .exceptions import CsiInternalError
from .log_util import temp_logger
from .xml_util import XmlUtil
from .datetime_util import DatetimeUtil


from ..sentinel.sentinel1_product import Sentinel1Product
from ..sentinel.sentinel2_product import Sentinel2Product


class CreodiasUtil(RequestUtil):
    '''
        Request input products in the Dias catalogue.
        We were previously using Creodias, and moved to Wekeo.
        Wekeo currently using the same API than Creodias.
        See:
         * https://creodias.eu/eo-data-finder-api-manual
         * https://finder.creodias.eu/resto/api/collections/Sentinel2/describe.xml
        '''

    # Request URL root
    URL_ROOT = {
        's1' : 'http://finder.creodias.eu/resto/api/collections/Sentinel1/search.json',
        's2' : 'http://finder.creodias.eu/resto/api/collections/Sentinel2/search.json'
    }

    # Number of results per page.
    URL_PAGE_SIZE = 200

    # First page index
    URL_FIRST_PAGE_INDEX = 1

    # URL parameter: geometry - region of interest, defined as WKT string (POINT, POLYGON, etc.)
    # in WGS84 projection.
    URL_PARAM_GEOMETRY = 'geometry'

    # URL parameters: publishedAfter, publishedBefore - the date limits when the product was
    #  published in our repository
    URL_PARAM_PUBLISHED_AFTER = 'publishedAfter'
    URL_PARAM_PUBLISHED_BEFORE = 'publishedBefore'

    # URL parameter : result page index.
    URL_PARAM_PAGE_INDEX = 'page'

    # Static URL parameters.
    # maxRecords: request n products per page.
    # productType: request only L1C or GRD products.
    # status 0: only request processed products that are waiting in the DIAS platform
    
    # sortParam='published' and sortOrder='ascending' are necessary to be sure that new products that arrive during the request
    # duration do not mess up product order in page i.e. this is the only way to be sure not to miss products
    
    URL_STATIC_PARAMS = {
        's1': {
            'productType': 'GRD',
            'status': '0',
            'processingLevel' : 'LEVEL1',
            'maxRecords': URL_PAGE_SIZE,
            'sortParam': 'published',
            'sortOrder': 'ascending'
        },
        's2': {
            'productType': 'L1C',
            'status': '0',
            'maxRecords': URL_PAGE_SIZE,
            'sortParam': 'published',
            'sortOrder': 'ascending'
        }
    }

    # Error message to be raised when the Creodias API is overloaded
    __CREODIAS_API_ERROR = "Too many request were sent to Creodias API, " \
                           "we should wait 1 minute before sending new requests"

    HTTP_CRITICAL_ERROR = [400, 409, 429]

    def __init__(self):
        self.mission_type = None

    def _populate_url(self, geometry, date_min, date_max, page, other_params=None):
        '''
        Build the URL used to perform the request.
        '''

        # URL parameters.
        # Min and max dates = date limits when the product was published in the DIAS repository.

        if not isinstance(page, list):
            geometry = [geometry]
            date_min = [date_min]
            date_max = [date_max]
            page = [page]

        url_params = [self.URL_STATIC_PARAMS[
            self.mission_type].copy() for _ in range(len(page))]

        for i in range(len(page)):
            if other_params is not None:
                url_params[i].update(other_params)
            if geometry[i] is not None:
                url_params[i][self.URL_PARAM_GEOMETRY] = geometry[i]
            if date_min[i] is not None:
                url_params[i][self.URL_PARAM_PUBLISHED_AFTER] = (
                    DatetimeUtil.toRfc3339(date_min[i]))
            if date_max[i] is not None:
                url_params[i][self.URL_PARAM_PUBLISHED_BEFORE] = (
                    DatetimeUtil.toRfc3339(date_max[i]))
            if page[i] is not None:
                url_params[i][self.URL_PARAM_PAGE_INDEX] = page[i]

        return url_params

    def catch_errors(self, exception, request_response, logger_func=None):
        '''
        Catch the error raised by the requests sent, and reformat them into
        human readable versions.
        '''

        exception_logger = logger_func if logger_func else temp_logger.error
        if isinstance(exception, (exceptions.ConnectionError, exceptions.HTTPError, urllib3.exceptions.NewConnectionError)):
            if isinstance(exception, exceptions.HTTPError) and (exception.response.status_code in self.HTTP_CRITICAL_ERROR):

                # Parse error message content
                error_content = exception.response.content.decode('ascii')

                error_subtype = "Unknown Communication Error"
                error_message = error_content

                # Display an error message if Creodias recieved too many requests
                if exception.response.status_code == 429:
                    exception_logger("%s : %s" % (
                        self.__CREODIAS_API_ERROR,
                        exception
                    ))
                    error_subtype = "Creodias API Overloaded"
                    error_message = f"{self.__CREODIAS_API_ERROR} " \
                                    f"Error : {error_content}"

                # Raise exception to stop services
                raise CsiInternalError(
                    error_subtype,
                    error_message
                ) from exception

            if isinstance(exception, exceptions.HTTPError) and (exception.response.status_code == 500):
                exception_logger(f"Creodias API seems unreachable : {exception}")
                return None

            # Display only one error message if it's an unknown Request error
            exception_logger(f"Unknown 'Request' exception occurred while "\
                f"requesting Creodias API : {exception}")
            return None

        elif isinstance(exception, json.decoder.JSONDecodeError):
            exception_logger(f"This request response could not be "\
                f"JSON decoded : '{request_response}', the following error "\
                f"occurred : \n    - error = {exception}")
            return None

        elif isinstance(exception, ElementTree.ParseError):
            exception_logger(f"Creodias API seems to be encountering troubles, "\
                f"wrong formatted answer received {exception}!")
            return None

        else:
            # Display other error messages
            exception_logger(f"Unknown exception occurred while requesting "\
                f"Creodias API : {exception}")
            return None

    def _send_and_check_request(self, url_params_page):
        '''
        Send request,
        Check response from the Python requests module,
        Raise exception with error message if the response status is !=OK
        '''
        # Send Get request, and re-send it until it succeed
        successfull_request = False
        response = None
        while not successfull_request:
            try:
                try:
                    if isinstance(url_params_page, dict):
                        response, url = self.send_request(
                            url=self.URL_ROOT[self.mission_type],
                            params=url_params_page,
                            logger_func=None,
                            return_url=True)

                        # Check if the request response can be decoded
                        if response is not None:
                            response.json()

                    else:
                        response, url = self.send_request(
                            url=url_params_page,
                            params=None,
                            logger_func=None,
                            return_url=True)

                    successfull_request = True

                except Exception as exception:
                    self.catch_errors(exception, response, logger_func=None)
            except CsiInternalError as exception:
                if exception.subtype == "Creodias API Overloaded":
                    time.sleep(60)
        return response

    def request(self,
                logger,
                mission_type,
                geometry,
                date_min,
                date_max,
                other_params=None,
                max_requested_pages=None,
                get_manifest=False):
        """ Performs a request to the DIAS HTTP API.

        Keyword arguments:
        mission_type -- mission_type [s1,s2]
        geometry -- wkt geometry for request AOI
        date_min -- Minimum publication date for request range
        date_max -- Maximum publication date for request range
        other_params -- other parameters as dict (optional e.g. {'productIdentifier':'%T31TCH%'} )
        max_requested_pages -- Maximum number of pages to request simultaneously (optional)
        get_manifest -- return manifest slice information (optional)
        """
        json_list = []
        # Number of requested pages
        requested_pages = 0

        if mission_type in self.URL_ROOT.keys():
            self.mission_type = mission_type
        else:
            logger.error("Unkown mission type")
            return

        def first_exit_condition():
            return (max_requested_pages is not None) and (requested_pages >= max_requested_pages)
        while not first_exit_condition():

            # n multithreaded requests at a time
            pages = []
            for _ in range(self.PARALLEL_REQUESTS):
                # Increment the number of requested pages
                pages.append(requested_pages+1)
                requested_pages += 1

                # Same as the first exit condition
                if first_exit_condition():
                    break

            response = self._request([geometry for _ in range(self.PARALLEL_REQUESTS)],
                                     [date_min for _ in range(self.PARALLEL_REQUESTS)],
                                     [date_max for _ in range(self.PARALLEL_REQUESTS)],
                                     page=pages,
                                     other_params=other_params)

            # Decode the request multiple responses
            json_aux = [x.json() for x in response if x is not None]

            # Second exit condition: no results are returned
            if not json_aux[-1]['properties'].get('itemsPerPage') > 0:
                break

            # Save results
            json_list += json_aux

        # Get features
        try:
            features = [x["features"] for x in json_list]
            features = list(itertools.chain.from_iterable(features))
        except KeyError:
            features = {}
            logger.error('features entry is missing from the JSON response:\n%s'% (
                                  json.dumps(json_list, indent=4))
                              )

        # TODO: Check for duplicate

        # Resulting Sentinel products
        products = []

        # Read each feature
        for feature_index, feature in enumerate(features):
            feature_read = self.read_input_product_feature(json_list, feature, feature_index, get_manifest=get_manifest)
            if feature_read is not None:
                products.append(feature_read)

        # Return the Sentinel products
        return products

    @staticmethod
    def read_input_product_feature(json_root: dict, feature: dict, feature_index: int, get_manifest: bool=True):
        '''
        Read a Sentinel-1|2 product JSON feature.

        :param json_root: JSON containing all the products.
        :param feature: "feature" parameter of a product.
        :param feature_index: index of the given "feature" parameter.
        :param get_manifest: return manifest slice information (optional).
        '''

        # Partial Python function. Then just add the json_param for each call.
        read = partial(
            CreodiasUtil.read_json_param,
            json_root, feature, feature_index)

        platform = read('platform')
        instrument = read('instrument')
        productType = read('productType')

        # Full L1C path in the DIAS catalogue
        product_path = read('productIdentifier')

        # Read each JSON parameter and create a new sentinel product object.
        cloud_cover = read('cloudCover')
        snow_cover = read('snowCover')

        # There is also the 'update' param that we don't use.
        dias_publication_date=DatetimeUtil.fromRfc3339(read('published'))


        product = None
        if platform.startswith("S2"):
            product = Sentinel2Product(product_path, dias_publication_date, cloud_cover, snow_cover)
        elif platform.startswith("S1"):
            # list to parse any additional fields and get them in the object
            mtd_key_list = ['orbitNumber',
                            'orbitDirection',
                            'relativeOrbitNumber',
                            'missionTakeId',
                            'timeliness',
                            'gmlgeometry',
                            'thumbnail',
                            'productIdentifier']
            S1Metadata = namedtuple('S1Metadata', mtd_key_list)
            mtd_info_list = []
            for key in mtd_key_list:
                mtd_info_list.append(read(key))
            s1_metadata = S1Metadata._make(mtd_info_list)
            manifest_url = None

            if get_manifest:
                # Now we need to retrieve the manifest content
                if s1_metadata.thumbnail is not None:

                    manifest_url = s1_metadata.thumbnail.split('.SAFE/')[0] + ".SAFE/manifest.safe"
                elif s1_metadata.productIdentifier is not None:
                    manifest_url = s1_metadata.productIdentifier.replace("/eodata", "https://finder.creodias.eu/files") + "/manifest.safe"
                
                if manifest_url is not None:
                    response = CreodiasUtil()._multiprocessed_function_with_timeout(
                        CreodiasUtil()._request_page, [manifest_url])[0]  

                    # Check if the request response can be decoded
                    if response is not None:
                        manifest_root = ElementTree.fromstring(response.content)

                        # XML namespaces
                        ns = XmlUtil.namespace(response.content)
                        # list to parse any manifest fields and get them in the object
                        manifest_key_list = ['sliceNumber',
                                            'totalSlices']
                        S1Manifest = namedtuple('S1Manifest', manifest_key_list)
                        manifest_info_list = []
                        for key in manifest_key_list:
                            element = './/s1sarl1:' + key
                            manifest_info_list.append(manifest_root.find(element, ns).text)
                        s1_manifest = S1Manifest._make(manifest_info_list)
                else:
                    return product
            else:
                s1_manifest = None

            product = Sentinel1Product(product_path, dias_publication_date, s1_metadata, manifest=s1_manifest)
        return product


    @staticmethod
    def read_json_param(json_root: dict, feature: dict, feature_index: int, json_param: str):
        '''
        Read a JSON parameter.

        :param json_root: JSON containing all the products.
        :param feature: "feature" parameter of a product.
        :param feature_index: index of the given "feature" parameter.
        :param json_param: parameter to read in the "feature".
        '''

        try:
            return feature['properties'][json_param]
        except KeyError:
            raise Exception(
                'features[%d][\'properties\'][\'%s\'] entry is missing from the JSON contents:\n%s' %
                (feature_index, json_param, json.dumps(json_root, indent=4)))
