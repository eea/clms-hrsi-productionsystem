'''
Class that gives access to the system parameters
It also contains emergency default values to be used in case of under specified system.
The values that are given here may be deprecated and are overcharged by the ones
on the sql file.
'''

import os
import time

from ...rest.rest_database import RestDatabase
from ....util.sys_util import SysUtil


class SystemPrameters(RestDatabase):
    '''
    Description of external parameters that can be set.
    
    :param max_number_of_worker_instances: maximum number of VMs that can be created by the worker.
    :param max_number_of_extra_large_worker_instances: maximum number of extra large VMs that can be created.
    :param job_types_list: list of job types handled by the orchestrator.
    :param s1_search_default_duration_in_days: value (in days) to use for a default
    backward research on S1 products.
    :param s1_processing_start_date: date on which the S1 products processing
    service started.
    :param s2_search_default_duration_in_days: value (in days) to use for the
     backward search of S2 products at the starting of the system, i.e. when no
     product has ever been processed by our processing and the database is empty.
    :param s2_search_default_duration_in_days: date on which the S2 products
    processing service started.
    :param maja_consecutive_jobs_threshold_value: Maximum time range (in days)
     between two jobs measurement dates to run MAJA with NOMINAL mode.
    :param maja_backward_required_job_number: Number of job data (L1C) required
     by MAJA to be able to run a BACKWARD process.
    :param activate_backward_reprocessing: Option to activate "backward" reprocessing
     of jobs, processed with MAJA "init" mode, or old ones.
    :param rabbitmq_communication_endpoint: address to send the JSON data to while
     publishing a product.
    :param gfsc_daily_jobs_creation_start_date: starting date from which we want to attempt 
     to re-create GFSC daily jobs.
    '''

    # Database table name
    __TABLE_NAME = "system_parameters"

    # Root url to send requests to
    URL_ROOT = os.path.join(f'{SysUtil.read_env_var("COSIMS_DB_HTTP_API_BASE_URL")}',__TABLE_NAME)

    def __init__(self, **kwds):
        self.max_number_of_worker_instances = 210
        self.max_number_of_vcpus = 920
        self.max_time_for_worker_without_nomad_allocation = 15 #in minutes
        self.max_ratio_of_vcpus_to_be_used = 0.95
        self.max_number_of_extra_large_worker_instances = 2
        self.job_types_list = "['fsc_rlie_job', \
                                'rlies1_job', \
                                'rlies1s2_job', \
                                'sws_wds_job', \
                                'gfsc_job']"
        self.s1_search_default_duration_in_days = 7
        self.s1_processing_start_date = "2021-06-24"
        self.s2_search_default_duration_in_days = 7
        self.s2_processing_start_date = "2020-05-01"
        self.gapfilling_search_default_duration_in_days = 7
        self.maja_consecutive_jobs_threshold_value = 60
        self.maja_backward_required_job_number = 8
        self.activate_backward_reprocessing = True
        self.rabbitmq_communication_endpoint = \
            "hidden_value"\
            ";hidden_value"\
            ";hidden_value"
        self.docker_image_for_si_processing = "hidden_value"
        self.docker_image_for_test_job_processing = "hidden_value"
        self.docker_image_for_rliepart2_processing = "hidden_value"
        self.docker_image_for_gfsc_processing = "hidden_value"
        self.docker_image_for_sws_wds_processing = "wetsnowprocessing:git-766faf2f"
        self.worker_init_package_tag = "hidden_value"
        self.ssp_aux_version = "V20211119"
        self.job_creation_loop_sleep = 5
        self.job_configuration_loop_sleep = 5
        self.job_execution_loop_sleep = 5
        self.job_publication_loop_sleep = 5
        self.rlies1s2_min_delay_from_end_of_day_hours = 8
        self.rlies1s2_max_delay_from_end_of_day_hours_wait_for_rlie_products = 24
        self.rlies1s2_min_search_window_days = 7
        self.rlies1s2_max_search_window_days = 31
        self.rlies1s2_max_search_window_days_absolute = None
        self.rlies1s2_earliest_date = "2021-05-01"
        self.rlies1s2_sleep_seconds_between_loop = None
        self.gfsc_daily_jobs_creation_start_date = None
        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(SystemPrameters.__TABLE_NAME)

        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)

    # Return a dictionnary containing all the class attributes paired with their values
    def class_attributes_as_dict(self):
        '''
        _summary_

        :return: _description_
        :rtype: _type_
        '''
        return self.__dict__

    # Post external parameters to database
    def post(self, logger_func=None):
        '''
        _summary_

        :param logger_func: _description_, defaults to None
        :type logger_func: _type_, optional
        :return: _description_
        :rtype: _type_
        '''
        # Set data to post according to class current values
        data = self.class_attributes_as_dict()

        # Post data
        response = super().post(
            data=data,
            logger_func=logger_func
        )
        return response

    # Retreive external parameters in the database
    def get(self, logger_func=None, error_count=0):
        '''
        _summary_

        :param logger_func: _description_, defaults to None
        :type logger_func: _type_, optional
        :param error_count: _description_, defaults to 0
        :type error_count: int, optional
        :return: _description_
        :rtype: _type_
        '''

        system_parameter = super().get(
            logger_func=logger_func,
        )

        if isinstance(system_parameter, list) and len(system_parameter)>0:
            system_parameter = system_parameter[0]
        else:
            if error_count < 3:
                time.sleep(5)
                return self.get(logger_func=logger_func, error_count=error_count + 1)
            elif logger_func:
                logger_func("Couldn't reach the database to retrieve System Parameters !")
                return None
            else:
                return None

        return system_parameter

    # Patch external parameters in the database
    def patch(self, logger_func=None):
        '''
        _summary_

        :param logger_func: _description_, defaults to None
        :type logger_func: _type_, optional
        '''

        # Set data to patch according to class current values
        data = self.class_attributes_as_dict()

        # Patch data
        super().patch(data=data, logger_func=logger_func)
