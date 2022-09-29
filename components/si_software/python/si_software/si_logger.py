#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import logging

from si_common.common_functions import get_and_del_param

from cosims.components.common.python.util.log_util import LogUtil


def get_logger(log_file_path=None, verbose_level=1, fsc_rlie_job_id=None, psa_arlie_job_id=None):
    '''
    Return a Python logger, depending on the environment.

    :param params: (dict) execution parameters
    :param log_file_path: (str) Log file path. If None: do not write a log file.
    '''
    
    if (psa_arlie_job_id is not None) and (fsc_rlie_job_id is not None):
        raise Exception("Can't have a fsc_rlie_job_id logger and a psa_arlie_job_id logger, must be one or the other that corresponds to the job identity")
    elif (psa_arlie_job_id is not None) or (fsc_rlie_job_id is not None):
        
        #########
        # old log used to communicate with orchestrator DB, no longer used, and must be reimplemented into the system if ever to be reused
        # ~ # Note: the COSIMS_DB_HTTP_API_BASE_URL environment variable must exist
        # ~ # and contain the URL to access the database, or these imports will fail.
        # ~ from cosims.components.common.python.database.logger import Logger
        
        # ~ if fsc_rlie_job_id is not None:
            # ~ # Get the FSC/RLIE job for which the job foreign key ID
            # ~ # equals the given value.
            # ~ from cosims.components.common.python.database.model.job.fsc_rlie_job import FscRlieJob
            # ~ jobs = FscRlieJob().id_eq(fsc_rlie_job_id).get()
            # ~ assert len(jobs) != 0, 'FSC/RLIE job ID is missing from the database: %s'%fsc_rlie_job_id
            # ~ assert len(jobs) == 1, 'FSC/RLIE job ID has been found multiple times in the database: %s'%fsc_rlie_job_id
        # ~ elif psa_arlie_job_id is not None:
            # ~ # Get the FSC/RLIE job for which the job foreign key ID
            # ~ # equals the given value.
            # ~ from cosims.components.common.python.database.model.job.psa_arlie_job import PsaArlieJob
            # ~ jobs = PsaArlieJob().id_eq(psa_arlie_job_id).get()
            # ~ assert len(jobs) != 0, 'PSA/ARLIE job ID is missing from the database: %s'%psa_arlie_job_id
            # ~ assert len(jobs) == 1, 'PSA/ARLIE job ID has been found multiple times in the database: %s'%psa_arlie_job_id

        # ~ # Create and return the custom logger instance
        # ~ return Logger(jobs[0].post_new_execution_info())
        #########
        raise Exception('fsc_rlie_job_id and psa_arlie_job_id parameters to allow logging to orchestrator DB are no longer used')

    # If the job database ID is not defined, use a native Python logger object
    else:
        
        log_level = None
        if verbose_level > 1:
            log_level = logging.DEBUG
        elif verbose_level > 0:
            log_level = logging.INFO

        # Create and return logger
        return LogUtil.get_logger(
            log_name='snow_ice',
            log_level=log_level,
            log_file_path=log_file_path)
