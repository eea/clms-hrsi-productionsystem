from datetime import datetime

from .job_template import JobTemplate
from .job_status import JobStatus
from .worker_flavors import WorkerFlavors


class TestJob(JobTemplate):
    '''
    Description of a generic test job, used for integration validation purpose.
    '''

    ################################################################
    # Start of specific properties

    # Database table name
    __TABLE_NAME = "test_jobs"

    # Class job name
    JOB_NAME = "test_job"

    # Sentinel input product type
    INPUT_PRODUCT_TYPE = None

    # Name of the Nomad job processing the present job type
    NOMAD_JOB_NAME = "test-job-processing"

    # Worker flavor required for the present job type processing
    WORKER_FLAVOR_NAME = WorkerFlavors.extra_small.value

    # Name of the products the job will generate during it's processing
    OUTPUT_PRODUCTS_LIST = ["test_product"]

    # Keep track of the last job inserted in the database.
    LAST_INSERTED_JOB = None

    # Name of the stored procedure used to retrieve TestJob with a given status
    GET_JOBS_WITH_STATUS_PROCEDURE_NAME = "test_jobs_with_last_status"

    ################################################################
    # End of specific properties


    def __init__(self, **kwds):
        self.measurement_date = None
        self.completion_date = None

        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(TestJob.__TABLE_NAME)

        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)


    ################################################################
    # Start of specific methods

    def get_last_inserted_job(self):
        '''
        Return the last job that has been inserted in the database.
        Note that this method is only usefull for jobs which are requesting external 
        APIs for new products, to ensure to not miss any data between two separated 
        requests. This function is called in the "compute_dias_request_time_range()" 
        method, if you don't need to request any external APIs, you don't need to define it.
        '''

        raise NotImplementedError


    def set_product_publication_date(self, product_name: str = None):
        '''
        Set the publication date of a given type of product.
        
        :param product_name: name of the product to update the publication date.
            This parameter is not mandatory, but can be used if several products 
            are generated by the same job to distinguish which product info should 
            be updated.
        '''

        return


    def generated_a_product(self):
        '''Return a boolean to notify if the job did generate a product or not.'''

        return False


    def job_pre_insertion_setup(self, reprocessed_job=False):
        '''
        Perform set up actions for a given job, before its insertion in database.

        :param reprocessed_job: boolean notifying if the job is an old job part 
            of a reprocessing campaign.
        '''

        return


    def set_job_unique_name(self):
        '''Set job unique name.'''

        if self.name is None:
            self.name = f"test_job-{self.measurement_date.strftime('%Y-%m-%dT%H:%M:%S')}"


    def configure_single_job(self, logger_func):
        '''
        Perform input product specific configuration, on a single job, required 
        for its processing.

        :param logger_func: Logger instance.
        '''

        return self, JobStatus.ready, None


    def get_products_publication_jsons(self, publication_json_template: dict):
        '''
        Fill the json to be sent to notify product publication.

        :param publication_json_template: JSON template to be filled before sending.
        '''

        return []


    ################################################################
    # Start of static specifics methods

    @staticmethod
    def get_jobs_to_create(internal_database_parallel_request: int, logger):
        '''
        Request an API for new input products and determine the list of jobs 
        which should be created from it. This list of job can be filtered and ordered.
        
        :param internal_database_parallel_request: number of requests which can 
            be performed in parallel to the database.
        :param logger: Logger instance.
        '''

        job = [
            TestJob(
                measurement_date = datetime.utcnow(),
                tile_id = "dummy_value",
            )
        ]

        reprocessed_job = []

        return job, reprocessed_job


    @staticmethod
    def configure_batch_jobs(jobs, logger_func):
        '''
        Perform input product specific configuration, on a batch of jobs, which
        are required for their processing.

        :param jobs: list of jobs to be configured.
        :param logger_func: Logger instance
        '''

        return jobs
