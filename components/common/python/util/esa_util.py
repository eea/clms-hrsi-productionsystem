import os
import time
import traceback
from requests.auth import HTTPBasicAuth
from xml.etree import ElementTree
from requests import exceptions

from .request_util import RequestUtil
from .datetime_util import DatetimeUtil
from .xml_util import XmlUtil
from .exceptions import CsiInternalError
from .log_util import temp_logger


class EsaUtil(RequestUtil):
    '''
    Request input product information in the ESA SciHub.
    See: https://scihub.copernicus.eu/userguide/OpenSearchAPI
    '''

    # Request URL root ('dhus' = for manual request with limited requests quota,
    # 'apihub' = for automated scripts).
    # URL_ROOT = 'https://scihub.copernicus.eu/dhus/search'
    URL_ROOT = 'https://apihub.copernicus.eu/apihub/search'

    # Number of results per page.
    URL_PAGE_SIZE = 20

    # Query parameters
    URL_PARAM_PAGE_SIZE = 'rows'
    URL_PARAM_QUERY = 'q'
    URL_PARAM_OR = ' OR '
    URL_PARAM_ID = 'identifier'
    URL_PARAM_INGESTION_DATE = 'ingestiondate'

    # Number of pages to request simultaneously
    PARALLEL_REQUESTS = 1

    # Login
    USER = 'cosims.magellium'
    # Ensure the password is set in the environment variables
    if 'CSI_SCIHUB_ACCOUNT_PASSWORD' not in os.environ:
        raise CsiInternalError(
            'Missing env var',
            "'CSI_SCIHUB_ACCOUNT_PASSWORD' environment variable not found")
    if os.environ['CSI_SCIHUB_ACCOUNT_PASSWORD'] == '':
        raise CsiInternalError(
            'Empty env var',
            "'CSI_SCIHUB_ACCOUNT_PASSWORD' is set but contains an empty string")
    # Password
    PASSWORD = os.environ['CSI_SCIHUB_ACCOUNT_PASSWORD']

    # Error message to be raised when the ESA SciHub API is overloaded
    __SCIHUB_API_ERROR = "Too many request were sent to ESA SciHub API, " \
                           "we should wait 1 minute before sending new requests"


    def _populate_url(self, ids):
        # URL query parameters
        params = {}

        # Number of results per page
        params[self.URL_PARAM_PAGE_SIZE] = self.URL_PAGE_SIZE

        # Input products IDs
        param_ids = [(self.URL_PARAM_ID + ':' + id) for id in ids]
        params[self.URL_PARAM_QUERY] = self.URL_PARAM_OR.join(param_ids)
        return params

    def catch_errors(self, exception, logger_func=None):
        '''
        Catch the error raised by the requests sent, and reformat them into
        human readable versions.
        '''

        exception_logger = logger_func if logger_func else temp_logger.error

        if isinstance(exception, (exceptions.HTTPError)):
            if exception.response.status_code == 429:
                # Parse error message content
                error_content = exception.response.content.decode('ascii')

                # Display an error message if ESA API recieved too many requests
                exception_logger("%s : %s" % (
                    self.__SCIHUB_API_ERROR,
                    exception
                ))
                error_subtype = "ESA SciHub API Overloaded"
                error_message = f"{self.__SCIHUB_API_ERROR} " \
                                f"Error : {error_content}"

                raise CsiInternalError(
                    error_subtype,
                    error_message
                ) from exception

            elif exception.response.status_code in [404, 500, 503]:
                exception_logger(f"ESA SciHub API is unreachable !")

        elif (isinstance(exception, (SyntaxError)) 
        and exception.args 
        and isinstance(exception.args, (list, tuple))
        and "prefix 'xmlns' not found in prefix map" in exception.args[0]
        or isinstance(exception, ElementTree.ParseError)):
            exception_logger(f"ESA SciHub API seems to be encountering troubles, "\
                f"wrong formatted answer received !")

        elif (exception.args 
        and isinstance(exception.args, (list, tuple))
        and "Reached max number of retry" in exception.args[0]):
            exception_logger(f"ESA SciHub API is unreachable !")

        else:
            # Display other error messages
            exception_logger(f"Unknown exception occurred : {traceback.format_exc()}")

    def _send_and_check_request(self, url_params_page):
        '''
        Send request,
        Check response from the Python requests module,
        Raise exception with error message if the response status is !=OK
        '''

        response = None

        try:
            try:
                response, url = self.send_request(
                    url=self.URL_ROOT,
                    params=url_params_page,
                    logger_func=None,
                    auth=HTTPBasicAuth(self.USER, self.PASSWORD),
                    return_url=True
                )
            except Exception as exception:
                self.catch_errors(exception, logger_func=None)
        except CsiInternalError as exception:
            if exception.subtype == "ESA SciHub API Overloaded":
                time.sleep(60)
        return response

    def request(self, ids):
        '''
        We will send Get requests to the ESA hub that contains all the product IDs to query.
        We split the job list into smaller lists so the sent URL is not too long.
        '''
        list_of_ids = []
        short_id = []
        for id in ids:
            if len(''.join(short_id + [id])) < 1000:
                short_id.append(id)
            else:
                list_of_ids.append(short_id)
                short_id = [id]
        list_of_ids.append(short_id)
        out = []
        try:
            for list_of_id in list_of_ids:
                response = self._request(list_of_id)
                if response is not None:
                    for r in response:
                        if r is not None:
                        
                            # Read XML response
                            xml_root = ElementTree.fromstring(r.content)

                            # XML namespaces
                            ns = XmlUtil.namespace(r.content)

                            # Get all input products
                            products = xml_root.findall("./xmlns:entry", ns)
                            # Create dictionary of {product_id, ingestion_date}
                            dates = {}
                            for product in products:
                                _id = product.find(
                                    "./xmlns:str[@name='%s']" % EsaUtil.URL_PARAM_ID, ns)
                                _date = product.find(
                                    "./xmlns:date[@name='%s']" % EsaUtil.URL_PARAM_INGESTION_DATE, ns)
                                if (_id is not None) and (_id.text) and (_date is not None) and (_date.text):
                                    dates[_id.text] = DatetimeUtil.fromRfc3339(_date.text)

                            out.append(dates)

        except Exception as exception:
            self.catch_errors(exception, logger_func=None)

        result = {}
        for d in out:
            result.update(d)
        return result
