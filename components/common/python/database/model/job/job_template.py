from datetime import datetime, timedelta

from .child_job import ChildJob
from .job_status import JobStatus
from .system_parameters import SystemPrameters
from ...rest.stored_procedure import StoredProcedure


class JobTemplate(ChildJob):
    '''
    Template class for the definition of any type of job. The mandatory function 
    which should be implemented in the new job type class are defined in this 
    template.
    '''

    ################################################################
    # Start of specific properties

    # Database table name
    __TABLE_NAME = "table_name_not_implemented"

    # Class job name
    JOB_NAME = "job_name_not_implemented"

    # Sentinel input product type
    INPUT_PRODUCT_TYPE = "input_product_type_not_implemented"

    # Name of the Nomad job processing the present job type
    NOMAD_JOB_NAME = "nomad_job_name_not_implemented"

    # Worker flavor required for the present job type processing
    WORKER_FLAVOR_NAME = "worker_flavor_name_not_implemented"

    # Name of the products the job will generate during it's processing
    OUTPUT_PRODUCTS_LIST = ["output_product_name_not_implemented"]

    # Keep track of the last job inserted in the database.
    LAST_INSERTED_JOB = None

    # Name of the stored procedure used to retrieve jobs with a given status
    GET_JOBS_WITH_STATUS_PROCEDURE_NAME = "procedure_name_not_implemented"

    ################################################################
    # End of specific properties


    def __init__(self, table_name=''):

        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(table_name)


    ################################################################
    # Start of common methods

    def attribute_in(self, key, values):
        '''
        Build a rest request to return objects which have a "key" parameter
        matching one of the "values" list.
        '''

        return super().attribute_in(key, values)


    # Build a string for PostgREST query parameters
    def job_id_in(self, values):
        '''
        return jobs which ID match the list of values passed in argument.

        :param values: list of job ID value, in integer format.
        '''

        return super().attribute_in('id', values)


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


    def set_product_publication_date(self, product_name: str = None, publication_json: dict = None):
        '''
        Set the publication date of a given type of product.

        :param product_name: name of the product to update the publication date.
            This parameter is ot mandatory, but can be used if several products
            are generated by the same job to distinguish which product info should
            be updated.
        '''

        raise NotImplementedError


    def generated_a_product(self):
        '''Return a boolean to notify if the job did generate a product or not.'''

        raise NotImplementedError


    def job_pre_insertion_setup(self, reprocessed_job=False):
        '''
        Perform set up actions for a given job, before its insertion in database.
        Can be left blank and just perform a "return" if not needed.

        :param reprocessed_job: boolean notifying if the job is an old job part
            of a reprocessing campaign.
        '''

        raise NotImplementedError


    def set_job_unique_name(self):
        '''
        Set a job unique name. This method is not mandatory, but can be used
        during the job configuration process to fill the job's "name" parameter
        (stored in the ParentJob table).
        '''

        raise NotImplementedError


    def configure_single_job(self, logger_func):
        '''
        Perform input product specific configuration, on a single job, required
        for its processing. The method should return the job itself, as well as
        the status it should have in the database, either "JobStatus.configured" if
        it needs to wait for any dependencies, or "JobStatus.ready" if it's ready
        to be processed.

        :param logger_func: Logger instance.
        '''

        raise NotImplementedError


    def get_products_publication_jsons(self, publication_json_template: dict):
        '''
        Fill the JSON template (dictionary object) to be sent to notify product
        publication. Can return a list of dictionaries if several products must
        be published.

        :param publication_json_template: dictionary to be filled before sending.
        '''

        raise NotImplementedError


    ################################################################
    # Start of static methods

    @staticmethod
    def compute_dias_request_time_range(job_type_class, logger):
        '''
        Compute the time range on which should be performed the request to the
        DIAS to look for new input products based on the last job inserted in
        the database. If no job were inserted yet we perform a backward search
        on a fixed amount of days in the past. This number of days is defined
        in the system parameters table for each type of job.

        :param job_type_class: specific job type class.
        :param logger: Logger instance.
        '''

        # Max date = now
        date_max = datetime.utcnow()

        # Min date = max publication date in the DIAS between all the
        # products that already exist in the database.
        # Only request database if we don't have information on last inserted job
        if job_type_class.LAST_INSERTED_JOB:
            job_dates = [job_type_class.LAST_INSERTED_JOB]
            last_inserted_job = None
        else:
            job_dates = job_type_class().get_last_inserted_job().get(logger_func=logger.debug)
            last_inserted_job = None
        if isinstance(job_dates, list) and len(job_dates) > 0:
            last_inserted_job = job_dates[0]
            date_min = last_inserted_job.get_input_product_dias_publication_date()

            # Remove 1 second in the case of multiple products were published
            # at the exact same moment of the last request and if the min date
            # is exclusive.
            date_min = date_min - timedelta(seconds=1)

        else:
            # This is a value (in days) to use for the backward search of input
            # products at the starting of the system, i.e. when no product
            # has ever been processed by our processing and the database is empty.
            input_product_search_default_duration_in_days = (
                job_type_class.get_input_product_search_default_duration_in_days(
                    job_type_class.INPUT_PRODUCT_TYPE,
                    logger.debug
                )
            )

            # For now, choose a starting date one month ago. The idea is to have
            # enough processing before the current date to have relatively good
            # L2A metadata quality.
            date_min = datetime.utcnow() - timedelta(days=input_product_search_default_duration_in_days)

        return date_min, date_max, last_inserted_job


    @staticmethod
    def get_input_product_search_default_duration_in_days(input_product_type, logger):
        '''
        return the input product search default duration (in days) stored in the
        system parameters table for a given input product type.
        Note that this method is only usefull for jobs which are requesting external
        APIs for new products, to get a backward search default duration if no jobs
        have been ingested in the database yet. This function is called in the
        "compute_dias_request_time_range()" method, if you don't need to request
        any external APIs, you don't need it.

        :param input_product_type: name of the input product type as a string.
        :param logger: logger.debug or logger.info or ...
        '''

        if input_product_type.lower() == 's1':
            return SystemPrameters().get(logger).s1_search_default_duration_in_days
        elif input_product_type.lower() == 's2':
            return SystemPrameters().get(logger).s2_search_default_duration_in_days


    @staticmethod
    def get_jobs_with_last_status(job_class, job_status: JobStatus, logger_func):
        '''
        return a list of jobs which exist in the database and which status
        currently match the one passed in argument.

        :param job_class: job type class.
        :param job_status: list of jobs to test if they exist in the database.
        :param logger_func: logger.debug or logger.info or ...
        '''

        return StoredProcedure.jobs_with_last_status(
            job_class, job_status, logger_func)


    ################################################################
    # Start of static specifics methods

    @staticmethod
    def get_jobs_to_create(internal_database_parallel_request: int, logger):
        '''
        Request an API for new input products and determine the list of jobs
        which should be created from it. Two lists should be returned by this
        method. The first one is the list of jobs which will be processed by
        the system. The second one is a list of jobs which are linked with
        input products that we don't want to process, either because they are
        too old, or because a first version of this input product has already
        been processed, and this second version is coming after a too long delay.
        These list of jobs can be filtered and ordered.

        :param internal_database_parallel_request: number of requests which can
            be performed in parallel to the database.
        :param logger: Logger instance.
        '''

        raise NotImplementedError


    @staticmethod
    def configure_batch_jobs(jobs, logger_func):
        '''
        Perform input product specific configuration, on a batch of jobs, which
        are required for their processing. The method should return a list of jobs.
        This method can be left blank if not needed, and should in this case
        return the list of jobs passed in argument.

        :param jobs: list of jobs to be configured.
        :param logger_func: Logger instance
        '''

        raise NotImplementedError
