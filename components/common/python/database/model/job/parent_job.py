import logging

from .job_priority import JobPriority
from ...rest.rest_database import RestDatabase
from ....util.log_util import temp_logger


class ParentJob(RestDatabase):
    '''
    Corresponds to the 'parent_jobs' SQL table.
    It holds attributes that are shared by child SQL tables : FSC/RLIE, PSA, ARLIE and Other jobs.
    
    The Snow and Ice production is divided into several jobs. 
    Each job is associated with a single Sentinel-2 tile. 
    It can be either the:
        * Generation of one FSC and one RLIE product from a single Sentinel-2 L1C product,
        * Generation of one PSA product from accumulated FSC products,
        * Generation of one ARLIE product from accumulated RLIE products.

    :param name: (str) Job name
    :param priority; (JobPriority)
    :param tile_id: (str) Sentinel-2 tile ID     
    :param next_log_level: Log level used for this next job execution. 
    Either logging.CRITICAL, ERROR, WARNING, INFO, or DEBUG
    :param next_log_file_path: (str) Log file path used for this next job execution.
    :param print_to_orch: (boolean) Print the log outputs into the orchestrator terminal. 
    Not yet implemented.
    :param logger: Logger instance
    :param last_status_id: ID of the current job status.
    :param last_status_change_id: ID of the last status change (as in JobStatusChange table).
    :param last_status_change_date: time of the last status change (as in JobStatusChange table).
    :param error_raised: (boolean) detect if a job encountered an error during its lifecycle.
    '''

    # Database table name
    TABLE_NAME = "parent_jobs"
    
    DEFAULT_LOG_LEVEL = logging.DEBUG
    #DEFAULT_LOG_LEVEL = logging.WARNING

    def __init__(self, log_level=None):
        '''
        :param log_level: Initial log level. Can be modified later by the operator for e.g. debugging jobs. 
        '''
        self.name = None
        self.unique_id = None
        self.priority = None
        self.tile_id = None
        self.next_log_level = log_level if log_level else ParentJob.DEFAULT_LOG_LEVEL
        self.next_log_file_path = None
        self.print_to_orch = None
        self.nomad_id = None
        self.last_status_id = None
        self.last_status_change_id = None
        self.last_status_change_date = None
        self.last_status_error_subtype = None
        self.error_raised = None
        self.si_processing_image = None

        # Call the parent constructor AFTER all the attributes are initialized
        super().__init__(ParentJob.TABLE_NAME)
        
        # Temp logger instance
        self.logger = temp_logger

    def from_database_value(self, attribute, value):
        '''Parse a string value as it was inserted in the database.'''

        if attribute == 'priority':
            return JobPriority[value]

        # Default: call parent class
        return super().from_database_value(attribute, value)
    
    