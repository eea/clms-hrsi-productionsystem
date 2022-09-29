from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired

from .rest_util import RestUtil
from .log_util import temp_logger

class RequestUtil(object):
    # Number of pages to request simultaneously
    PARALLEL_REQUESTS = 1

    def _populate_url(self, *args, **kwargs):
        raise NotImplementedError()

    def _send_and_check_request(self, *args, **kwargs):
        raise NotImplementedError()

    def request(self, *args, **kwargs):
        raise NotImplementedError()

    def _request(self, *args, **kwargs):
        # URL parameters.
        url_params = self._populate_url(*args, **kwargs)
        if not isinstance(url_params, list):
            url_params = [url_params]
        # Run the multithreaded requests and wait for finish.
        # Responses from the multithreaded requests: a list of lists of resulting jobs
        response = self._multiprocessed_function_with_timeout(self._request_page, url_params)
        return response

    def _multiprocessed_function_with_timeout(self, func, args, timeout=60, max_retry=5):
        index = list(range(len(args)))
        out = [None for _ in range(len(args))]

        n_try = -1
        while len(args) > 0:
            n_try += 1
            with ProcessPool() as pool:
                future = pool.map(func, args, timeout=timeout)
                iterator = future.result()
                i = -1
                failed = []
                failed_index = []
                while True:
                    i += 1
                    try:
                        result = next(iterator)
                        out[index[i]] = result
                    except StopIteration:
                        break
                    except TimeoutError:
                        failed.append(args[i])
                        failed_index.append(index[i])
                    except Exception as err:
                        temp_logger.error(f"RequestUtil error : {err} with properties : {dir(err)}")
                        if err.args:
                            temp_logger.error(f"RequestUtil error args : {err.args}")
                        if err.errno:
                            temp_logger.error(f"RequestUtil error errno : {err.errno}")
                        if err.exitcode:
                            temp_logger.error(f"RequestUtil error exitcode : {err.exitcode}")
                        if err.strerror:
                            temp_logger.error(f"RequestUtil error strerror : {err.strerror}")
                        if err.with_traceback:
                            temp_logger.error(f"RequestUtil error traceback : {err.with_traceback}")
                        raise err
            args = failed
            index = failed_index
            if len(failed) > 0:
                temp_logger.warning('Requests timeout [%d/%d]. Retry [%d/%d]' % (len(failed), len(args), n_try, max_retry))

            if n_try >= max_retry > 0:
                raise Exception('Request Error: Reached max number of retry.')

        return out

    def _request_page(self, arg):
        '''Request one page of input products (each page contains URL_PAGE_SIZE products).'''
        response = self._send_and_check_request(arg)
        return response


    def send_request(self, url, params, **kwargs):
        '''
        Send request,
        Check response from the Python requests module,
        Raise exception with error message if the response status is !=OK
        '''
        return RestUtil().get(url=url, params=params, **kwargs)