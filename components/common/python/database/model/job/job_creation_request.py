'''
_summary_
'''

import os

from ...rest.rest_database import RestDatabase
from ....util.sys_util import SysUtil


class JobCreationRequest(RestDatabase):
    '''
    Description of a job for the generation of one PSA product from accumulated FSC products.

    :param psa_arlie_creation_id: PSA/ARLIE product creation ID.
    :param request_status: request status.
    :param create_job: job to be created (PSA/ARLIE).
    :param hydro_year: start date of the hydrological year for PSA product.
    :param month: month over which RLIE porduct were generated to produce ARLIE result.
    '''

    # Database table name
    TABLE_NAME = "job_creation_request"

    # Root url to send requests to
    URL_ROOT = os.path.join(f"{SysUtil.read_env_var('COSIMS_DB_HTTP_API_BASE_URL')}",TABLE_NAME)

    def __init__(self, **kwds):
        self.psa_arlie_creation_id = None
        self.request_status = None
        self.create_job = None
        self.hydro_year = None
        self.month = None

        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(JobCreationRequest.TABLE_NAME)

        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)
