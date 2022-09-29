from enum import Enum


class JobStatus(Enum):
    '''
    Job processing status. Possible values are:
    * INITIALIZED, the job task is opened related to an input product
    * CONFIGURED, the job task is configured and is waiting for a dependency to be completed
    * READY, the job task is complete and is waiting for processing
    * QUEUED, the job has been queued to run on an available worker
    * STARTED, the job is being handled by the worker manager
    * PRE_PROCESSING, set up environment to run the worker
    * PROCESSING, the job task was assigned to a worker
    * POST_PROCESSING, execute post run actions
    * PROCESSED, the task was executed with success up to product generation
    * START_PUBLICATION, attempting to publish resulting products on the DIAS
    * PUBLISHED, the resulting products were published and configured through the DIAS data access
    * DONE, no action left to perform on the job
    * INTERNAL_ERROR, the job execution failed, because of an internal error
    * EXTERNAL_ERROR, the job execution failed, because of an external error
    * ERROR_CHECKED, the error has been handled
    * CANCELLED, threshold value has been reached, the job execution failed each time it was attempted,
                we don't try to run it anymore, no products were generated
    '''
    
    # Lower-case, as in the database
    initialized = 1
    configured = 2
    ready = 3
    queued = 4
    started = 5
    pre_processing = 6
    processing = 7
    post_processing = 8
    processed = 9
    start_publication = 10
    published = 11
    done = 12
    internal_error = 13
    external_error = 14
    error_checked = 15
    cancelled = 16