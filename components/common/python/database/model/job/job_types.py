"""
Class listing the orchestrator jobs.
"""

import json

from .fsc_rlie_job import FscRlieJob
from .rlies1_job import RlieS1Job
from .rlies1s2_job import RlieS1S2Job
from .sws_wds_job import SwsWdsJob
from .gfsc_job import GfscJob
from .test_job import TestJob
from .system_parameters import SystemPrameters
from ....util.exceptions import CsiInternalError

class JobTypes():
    '''
    Provide the list of supported job.
    Each job providers shall register their job type here.
    '''

    @staticmethod
    def get_job_type_list(logger):
        '''
        Get the current job type list from the system parameters table.
        
        :param logger: logger instance.
        '''

        
        system_parameters = SystemPrameters().get(logger.debug)
        if system_parameters is not None:
            job_type_list_string = system_parameters.job_types_list
            job_type_list = json.loads(job_type_list_string)
        else:
            job_type_list = []

        job_type_class_list = []

        for job_type in job_type_list:
            if job_type.lower() == "fsc_rlie_job":
                job_type_class_list.append(FscRlieJob)
            elif job_type.lower() == "rlies1_job":
                job_type_class_list.append(RlieS1Job)
            elif job_type.lower() == "rlies1s2_job":
                job_type_class_list.append(RlieS1S2Job)
            elif job_type.lower() == "sws_wds_job":
                job_type_class_list.append(SwsWdsJob)
            elif job_type.lower() == "gfsc_job":
                job_type_class_list.append(GfscJob)
            elif job_type.lower() == "test_job":
                job_type_class_list.append(TestJob)
            else:
                logger.error(f"Unknown job type set in system parameters table : {job_type}")
                raise CsiInternalError

        return job_type_class_list
