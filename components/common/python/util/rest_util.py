from requests import Session, Request
from .log_util import temp_logger


class RestUtil(object):
    '''
    Utility functions for REST requests.

    :param session: HTTP session
    '''

    # Max URL size (number of characters)
    MAX_URL_SIZE = 2000

    def __init__(self, session=None):
        if not session:
            session = Session()
        self.session = session

    def __prepare_request(self, **kwargs):
        '''
        Prepare request.
        See https://2.python-requests.org//en/latest/user/advanced/#prepared-requests
        '''
        return self.session.prepare_request(Request(**kwargs))

    def __request(self, logger_func=None, return_url=False, **kwargs):
        '''
        Send request, check status (raise exception if status !=OK), return response.

        :param method: HTTP method to use.
        :param url: Root of the URL to send.
        :param params: URL parameters to append to the URL. If a dictionary or
        list of tuples ``[(key, value)]`` is provided, form-encoding will
        take place.
        :param data: (dict) POST data.
        :param logger_func: logger.debug or logger.info or ...
        :param return_url: True to return the full URL (for debugging)
        :return: response or (response, full_url) if return_url is True
        '''

        # urllib3.disable_warnings()

        # Prepare request
        request = self.__prepare_request(**kwargs)
        # Log message if the logger function is defined
        if logger_func:

            # Format data
            try:
                data = '\n%s' % kwargs['data']
            except Exception:
                data = ''

            # Format and log message
            message = '%s request: %s%s' % (kwargs['method'], request.url, data)
            temp_logger.debug(message)

        # Check size
        if len(request.url) > self.MAX_URL_SIZE:
            raise Exception('URL is too long: %s' % request.url)

        try:
            # Send request and check response
            response = self.session.send(request)
            response.raise_for_status()
        except Exception as e:
            raise e

        if response is None:
            return None
        elif return_url:
            return (response, request.url)
        else:
            return response

    def get(self, **kwargs):
        '''
        Send a Get request.
        :see: __request.
        '''
        return self.__request(method='GET', **kwargs)

    def post(self, logger_func=None, **kwargs):
        '''
        Send a Post request.
        :see: __request.
        '''
        return self.__request(logger_func=logger_func, method='POST', **kwargs)

    def patch(self, logger_func=None, **kwargs):
        '''
        Send a Patch request.
        :see: __request.
        '''
        return self.__request(logger_func=logger_func, method='PATCH', **kwargs)

    def format_url(self, url, params):
        '''Format and return an URL with params.'''

        # Prepare request with a dummy Get method (not used)
        request = self.__prepare_request(method='GET', url=url, params=params)
        return request.url