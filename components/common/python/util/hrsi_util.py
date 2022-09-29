import time
import urllib3
import json
import itertools
from functools import partial
from requests import exceptions

from .request_util import RequestUtil
from .exceptions import CsiInternalError
from .log_util import temp_logger
from .datetime_util import DatetimeUtil

from .hrsi_product import HrsiProduct


class HrsiUtil(RequestUtil):
    '''
        Request input products in the HR-S&I catalogue.
        See:
         * https://cryo.land.copernicus.eu/finder/
         * https://cryo.land.copernicus.eu/resto/api/collections/HRSI/describe.xml
        '''

    # Request URL root
    URL_ROOT = 'https://cryo.land.copernicus.eu/resto/api/collections/HRSI/search.json'
    # TODO [Critical] Just for test env 
    # URL_ROOT = 'https://magellium.dev.cloudferro.com/resto/api/collections/HRSI/search.json'


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
    URL_STATIC_PARAMS = {
        'fsc': {
            'productType': 'FSC',
            'status': '0',
            'maxRecords': URL_PAGE_SIZE,
            'sortParam': 'published'  # may be useless: it's the default value used by the Dias
        },
        'rlie': {
            'productType': 'RLIE',
            'status': '0',
            'maxRecords': URL_PAGE_SIZE,
            'sortParam': 'published'  # may be useless: it's the default value used by the Dias
        },
        'wds': {
            'productType': 'WDS',
            'status': '0',
            'maxRecords': URL_PAGE_SIZE,
            'sortParam': 'published'  # may be useless: it's the default value used by the Dias
        },
        'sws': {
            'productType': 'SWS',
            'status': '0',
            'maxRecords': URL_PAGE_SIZE,
            'sortParam': 'published'  # may be useless: it's the default value used by the Dias
        },
        'gfsc': {
            'productType': 'GFSC',
            'status': '0',
            'maxRecords': URL_PAGE_SIZE,
            'sortParam': 'published'  # may be useless: it's the default value used by the Dias
        }
    }

    # Error message to be raised when the Creodias API is overloaded
    __HRSI_API_ERROR = "Too many request were sent to HR-S&I API, " \
                           "we should wait 1 minute before sending new requests"

    HTTP_CRITICAL_ERROR = [400, 409, 429]

    def __init__(self):
        self.product_type = None

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
            self.product_type].copy() for _ in range(len(page))]

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
                        self.__HRSI_API_ERROR,
                        exception
                    ))
                    error_subtype = "HR-S&I API Overloaded"
                    error_message = f"{self.__HRSI_API_ERROR} " \
                                    f"Error : {error_content}"

                # Raise exception to stop services
                raise CsiInternalError(
                    error_subtype,
                    error_message
                ) from exception

            # Display only one error message if it's an unknown Request error
            exception_logger("Unknown 'Request' exception occurred while "\
                "requesting Creodias API : %s" % (exception))
            return None

        elif isinstance(exception, json.decoder.JSONDecodeError):
            exception_logger(f"This request response could not be "\
                f"JSON decoded : '{request_response}', the following error "\
                f"occurred : \n    - error = {exception}")
            return None

        else:
            # Display other error messages
            exception_logger("Unknown exception occurred while requesting "\
                "HR-S&I API : %s" % (exception))
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
                    response, url = self.send_request(
                        url=self.URL_ROOT,
                        params=url_params_page,
                        logger_func=None,
                        return_url=True)

                    # Check if the request response can be decoded
                    if response is not None:
                        response.json()

                    successfull_request = True

                except Exception as exception:
                    self.catch_errors(exception, response, logger_func=None)
            except CsiInternalError as exception:
                if exception.subtype == "HR-S&I API Overloaded":
                    time.sleep(60)
        return response

    def request(self,
                logger,
                product_type,
                geometry,
                date_min,
                date_max,
                other_params=None,
                max_requested_pages=None):
        """ Performs a request to the HR-S&I HTTP API.

        Keyword arguments:
        product_type -- product_type [fsc,rlie]
        geometry -- wkt geometry for request AOI
        date_min -- Minimum publication date for request range
        date_max -- Maximum publication date for request range
        other_params -- other parameters as dict (optional e.g. {'productIdentifier':'%T31TCH%'} )
        max_requested_pages -- Maximum number of pages to request simultaneously (optional)
        """
        json_list = []
        # Number of requested pages
        requested_pages = 0

        if product_type in self.URL_STATIC_PARAMS.keys():
            self.product_type = product_type
        else:
            logger.error("Unkown product type")
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
            # TODO check why pages is not the same length as the built geometry.
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
            products.append(
                self.read_input_product_feature(json_list, feature, feature_index))

        # Return the Sentinel products
        return products

    @staticmethod
    def read_input_product_feature(json_root: dict, feature: dict, feature_index: int):
        '''
        Read a HR-S&I product JSON feature.

        :param json_root: JSON containing all the products.
        :param feature: "feature" parameter of a product.
        :param feature_index: index of the given "feature" parameter.
        '''

        # Partial Python function. Then just add the json_param for each call.
        read = partial(
            HrsiUtil.read_json_param,
            json_root, feature, feature_index)

        # Full path in the DIAS catalogue
        product_path = read('productIdentifier')
        product_type = read('productType')
        cloud_cover = read('cloudCover')
        snow_cover = read('snowCover')
        mission_type = read('mission')
        gml_geometry = read('gmlgeometry')
        measurement_date = DatetimeUtil.fromRfc3339(read('startDate'))
        creation_date = DatetimeUtil.fromRfc3339(read('completionDate'))

        # There is also the 'update' param that we don't use.
        dias_publication_date=DatetimeUtil.fromRfc3339(read('published'))

        return HrsiProduct(product_path, product_type, cloud_cover, snow_cover, measurement_date, creation_date, dias_publication_date, mission_type=mission_type, gml_geometry=gml_geometry)


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
