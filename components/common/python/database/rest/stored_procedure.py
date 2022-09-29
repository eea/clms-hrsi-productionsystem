import json
import signal

from ..model.execution.execution_message import ExecutionMessage
from ..model.job.job_status import JobStatus
from ..model.job.parent_job import ParentJob
from ...database.rest.rest_database import RestDatabase
from ...util.python_util import PythonUtil
from ...util.exceptions import TimeoutException, CsiExternalError
from ...util.log_util import temp_logger


class StoredProcedure(RestDatabase):
    '''
    Call a PostgREST stored procedure. 
    See https://postgrest.org/en/v4.1/api.html#stored-procedures
    '''

    #
    # Common functions

    def raise_timeout(self, signum, frame):
        '''Function to raise the TimeoutException on the signal.'''
        raise TimeoutException

    @staticmethod
    def call(response_type_instance, procedure_name, set_timeout, logger_func=None, **kwargs):
        '''
        Call a stored procedure

        :param response_type_instance: one instance of the response type
        :param procedure_name: procedure name, as specified in our Postgres database
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        :param logger_func: logger.debug or logger.info or ...
        :param kwargs: procedure arguments 
        '''

        # Build the table name, which will be used in the RestDatabse module to 
        # build the URI. Stored procedures are accessible under the /rpc prefix.
        table_name = 'rpc/%s' % procedure_name

        # Keep only the arguments that are defined, e.g. which value is not None.
        data = {key: value for key, value in kwargs.items() if value is not None}

        # From the PostgREST documentation:
        # You can also call a function that takes a single parameter of type json
        # by sending the header Prefer: params=single-object with your request.
        # That way the JSON request body will be used as the single argument.
        data = json.dumps(data)
        table_name += '?single-object'

        # Send stored procedure request, and re-send it until it succeed
        successfull_request = False
        response = None
        max_retry = 5
        retry_cpt = 0
        while not successfull_request:

            if set_timeout:
                # Register a function to raise a TimeoutException on the signal.
                signal.signal(signal.SIGALRM, StoredProcedure().raise_timeout)
                # Schedule the signal to be sent after ``1 min``.
                signal.alarm(60)
            if retry_cpt >= max_retry:
                raise CsiExternalError("Stored Procedure Request Error",
                    f"Reached max number of retry for procedure '{procedure_name}' : {max_retry}.")

            try:
                # Call the stored procedure
                response = RestDatabase(table_name).post(
                    data=data,
                    logger_func=logger_func,
                    parse_response=False)
                successfull_request = True

                # Parse objects contained in the response.
                return RestDatabase.parse_response(response, response_type_instance)
            except TimeoutException:
                retry_cpt += 1
                temp_logger.warning(f"StoredProcedure request timeout ({procedure_name}). "\
                    f"Retry [{retry_cpt}/{max_retry}]")
            finally:
                if set_timeout:
                    # Unregister the signal so it won't be triggered
                    # if the timeout is not reached.
                    signal.signal(signal.SIGALRM, signal.SIG_IGN)

    @staticmethod
    def join(children, foreigns):
        '''
        Complete the child instances with foreign instances, linked by a foreign key.
        Use this method when the SQL function cannot return the result of a join query.

        :param children: [ForeignKey] child instances
        :param foreigns: [RestDatabase] foreign instances
        '''

        # At least one child and foreign instance must exist
        if (not children) or (not foreigns):
            return

        # All children must be of the same type
        types = set([type(child) for child in children])
        if (len(types) != 1):
            raise Exception('Joined children must be of the same type: %s' % types)
        child_instance = children[0]

        # Same for foreign instances
        types = set([type(foreign) for foreign in foreigns])
        if (len(types) != 1):
            raise Exception('Joined foreign instances must be of the same type: %s' % types)
        foreign_instance = foreigns[0]

        # For each child foreign attribute
        for foreign_attr in child_instance.foreign_attrs:

            # Must be the same type as the foreign instances
            if not isinstance(foreign_instance, foreign_attr.type):
                continue

            # Order the foreign instances by their ID
            foreign_dict = {foreign.get_id(): foreign for foreign in foreigns}

            # For each child, find its foreign instance by joining the child foreign key and the foreign ID
            for child in children:
                foreign_key_value = child.get_foreign_id(foreign_attr)
                try:
                    foreign = foreign_dict[foreign_key_value]
                except Exception:
                    raise Exception(
                        'Child entry \'%s.%s=%s\' exists but foreign entry \'%s.%s=%s\' is missing' % (
                            child.table_name, foreign_attr.foreign_id, foreign_key_value,
                            foreign_attr.foreign_table, foreign_instance.id_name, foreign_key_value))
                    
                # Update the child foreign instance
                child.set_foreign_object(foreign_attr, foreign)

    #
    # Call stored procedures

    #
    # Messages

    @staticmethod
    def messages_with_body(body, logger_func, set_timeout=True):
        '''Get existing messages with the given body.'''

        # returns table (id int, body text)
        return StoredProcedure.call(
            response_type_instance=ExecutionMessage(),
            procedure_name='messages_with_body',
            body_arg=body,
            set_timeout=set_timeout,
            logger_func=logger_func)

    #
    # Status

    class JobStatusChange(RestDatabase):
        '''returns table (parent_job_id bigint, job_name text, status smallint, status_name text, change_id bigint, "time" timestamp)'''

        def __init__(self):
            self.parent_job_id = None
            self.job_name = None
            self.status = None
            self.status_name = None
            self.change_id = None
            self.time = None

            # Call the parent constructor AFTER all the attributes are initialized
            super().__init__()

        def from_database_value(self, attribute, value):
            '''Parse a string value as it was inserted in the database.'''

            if attribute == 'status':
                return JobStatus(value)

            # Default: call parent class
            return super().from_database_value(attribute, value)

    @staticmethod
    def job_status_history(parent_job_ids, logger_func, set_timeout=True):
        '''
        Get all status changes associated with parent job IDs.
        If job IDs are not defined: return results for all parent jobs.

        :param parent_job_ids: [integer]
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # IDs must be in a list
        parent_job_ids = PythonUtil.ensure_in_list(parent_job_ids, convert_none=False)

        return StoredProcedure.call(
            response_type_instance=StoredProcedure.JobStatusChange(),
            procedure_name='job_status_history',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

            

    @staticmethod
    def last_job_status(parent_job_ids, last_status, logger_func, set_timeout=True):
        '''
        Get last status changes associated with parent job IDs.
        If job IDs are not defined: return results for all parent jobs.
        If last status list is defined : only keep results with this these last status.

        :param parent_job_ids: [integer]
        :param last_status: [JobStatus]
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # IDs must be in a list
        parent_job_ids = PythonUtil.ensure_in_list(parent_job_ids, convert_none=False)
        last_status = PythonUtil.ensure_in_list(last_status, convert_none=False)

        # Convert enum status to integer
        last_status = [enum.value for enum in last_status]

        return StoredProcedure.call(
            response_type_instance=StoredProcedure.JobStatusChange(),
            procedure_name='last_job_status',
            parent_job_ids=parent_job_ids,
            last_status=last_status,
            set_timeout=set_timeout,
            logger_func=logger_func)



    @staticmethod
    def jobs_with_last_status(job_class, last_status, logger_func, set_timeout=True):
        '''
        Get the type of jobs passed in argument with the given last status
        (can be a list).

        :param job_class: any type of job class, for instance : FscRlieJob
        :param last_status: [JobStatus]
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # IDs must be in a list
        last_status = PythonUtil.ensure_in_list(last_status, convert_none=False)

        # Convert enum status to integer
        last_status = [enum.value for enum in last_status]

        # Get the FSC/RLIE jobs
        fsc_rlie_jobs = StoredProcedure.call(
            response_type_instance=job_class(),
            procedure_name=job_class.GET_JOBS_WITH_STATUS_PROCEDURE_NAME,
            last_status=last_status,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in fsc_rlie_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(fsc_rlie_jobs, parent_jobs)

        return fsc_rlie_jobs

    @staticmethod
    def fsc_rlie_jobs_with_status_tile_date(last_status, tile_id, low_time_bound, 
            high_time_bound, fsc_rlie_job_object, logger_func, set_timeout=True):
        '''
        Get FSC/RLIE jobs with the given last status (can be a list), tile id 
        and with a measurement date within a specific range.

        :param last_status: [JobStatus]
        :param tile_id: tile ID as a string
        :param low_time_bound: Datetime object
        :param high_time_bound: Datetime object
        :param fsc_rlie_job_object: FscRlieJob()
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # Status IDs must be in a list
        last_status = PythonUtil.ensure_in_list(last_status, convert_none=False)

        # Convert enum status to integer
        last_status = [enum.value for enum in last_status]

        # Get the FSC/RLIE jobs
        fsc_rlie_jobs = StoredProcedure.call(
            response_type_instance=fsc_rlie_job_object,
            procedure_name='fsc_rlie_jobs_with_status_tile_date',
            last_status=last_status,
            tile_id_ref=tile_id,
            low_time_bound=low_time_bound,
            high_time_bound=high_time_bound,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in fsc_rlie_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(fsc_rlie_jobs, parent_jobs)

        return fsc_rlie_jobs

    @staticmethod
    def get_jobs_within_measurement_date(job_class, date_parameter_name, start_date, 
            end_date, logger_func, set_timeout=True):
        '''
        Get jobs which specified date parameter remains within a provided time range.

        :param job_class: any type of job class, for instance : FscRlieJob,
        :param date_parameter_name: name of the date parameter on which should be
            performed the selection,
        :param start_date: Datetime object, low time bound,
        :param end_date: Datetime object, high time bound,
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''
        # Get the jobs
        jobs = StoredProcedure.call(
            response_type_instance=job_class,
            procedure_name='select_date_%s_%s'%(
                job_class.JOB_NAME.replace('/','_').lower(),
                date_parameter_name),
            start_date=start_date.strftime('%Y-%m-%dT%H:%M:%S'),
            end_date=end_date.strftime('%Y-%m-%dT%H:%M:%S'),
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(jobs, parent_jobs)

        return jobs


    @staticmethod
    def fsc_rlie_jobs_with_tile_date(tile_id, measurement_time, fsc_rlie_job_object,
            logger_func, set_timeout=True):
        '''
        Get FSC/RLIE jobs with the given tile id, and measurement date.

        :param tile_id: tile ID as a string
        :param measurement_time: Datetime object
        :param fsc_rlie_job_object: FscRlieJob()
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # Get the FSC/RLIE jobs
        fsc_rlie_jobs = StoredProcedure.call(
            response_type_instance=fsc_rlie_job_object,
            procedure_name='fsc_rlie_jobs_with_tile_date',
            tile_id_ref=tile_id,
            measurement_time=measurement_time,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in fsc_rlie_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(fsc_rlie_jobs, parent_jobs)

        return fsc_rlie_jobs

    @staticmethod
    def get_last_job_with_usable_l2a(
        tile_id, high_measurement_time_bound, high_esa_time_bound, l1c_id,
        allow_codated_jobs, backward_triggered_job, fsc_rlie_job_object, logger_func, set_timeout=True):
        '''
        Get FSC/RLIE job with most recent measurement date, inferior to the
        high time bound, with given tile id, and which didn't fail to generate
        an L2A product yet.

        :param tile_id: tile ID as a string
        :param high_measurement_time_bound: Datetime object
        :param high_esa_time_bound: Datetime object
        :param high_esa_time_bound: Datetime object
        :param l1c_id: L1C ID as a string
        :param allow_codated_jobs: Boolean, to allow taking as a reference an older 
            codated version of the current job L1C
        :param backward_triggered_job: Boolean, to identify if the current job has 
            been triggered by a backward reprocessing or not
        :param fsc_rlie_job_object: FscRlieJob()
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # Get the FSC/RLIE jobs
        fsc_rlie_jobs = StoredProcedure.call(
            response_type_instance=fsc_rlie_job_object,
            procedure_name='get_last_job_with_usable_l2a',
            tile_id_ref=tile_id,
            high_measurement_time_bound=high_measurement_time_bound,
            high_esa_time_bound=high_esa_time_bound,
            l1c_id_ref=l1c_id,
            allow_codated_jobs=allow_codated_jobs,
            backward_triggered_job=backward_triggered_job,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in fsc_rlie_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(fsc_rlie_jobs, parent_jobs)

        return fsc_rlie_jobs

    @staticmethod
    def fsc_rlie_jobs_following_measurement_with_tile_id(tile_id, low_measurement_time_bound, 
            low_l1c_esa_creation_time_bound, results_limit, fsc_rlie_job_object, logger_func, set_timeout=True):
        '''
        Get FSC/RLIE closest jobs, with measurement date more recent than the
        low time bound, and focusing on the specified tile id. Only return a
        specifid number of results, set by the 'results_limit' attribute.

        :param tile_id: tile ID as a string
        :param low_measurement_time_bound: Datetime object, job measurement date
        :param low_l1c_esa_creation_time_bound: Datetime object, job L1C esa creation date
        :param results_limit: integer
        :param fsc_rlie_job_object: FscRlieJob()
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # Get the FSC/RLIE jobs
        fsc_rlie_jobs = StoredProcedure.call(
            response_type_instance=fsc_rlie_job_object,
            procedure_name='fsc_rlie_jobs_following_measurement_with_tile_id',
            tile_id_ref=tile_id,
            low_measurement_time_bound=low_measurement_time_bound,
            low_l1c_esa_creation_time_bound=low_l1c_esa_creation_time_bound,
            results_limit=results_limit,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in fsc_rlie_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(fsc_rlie_jobs, parent_jobs)

        return fsc_rlie_jobs

    @staticmethod
    def fsc_rlie_job_last_init_with_tile_id_no_backward(tile_id, high_time_bound, 
            fsc_rlie_job_object, logger_func, set_timeout=True):
        '''
        Get last FSC/RLIE job processed with MAJA 'init' mode, with measurement
        date inferior to the high time bound, and focusing on the specified tile.

        :param tile_id: tile ID as a string
        :param high_time_bound: Datetime object
        :param fsc_rlie_job_object: FscRlieJob()
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # Get the FSC/RLIE jobs
        fsc_rlie_jobs = StoredProcedure.call(
            response_type_instance=fsc_rlie_job_object,
            procedure_name='fsc_rlie_job_last_init_with_tile_id_no_backward',
            tile_id_ref=tile_id,
            high_time_bound=high_time_bound,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in fsc_rlie_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(fsc_rlie_jobs, parent_jobs)

        return fsc_rlie_jobs


    @staticmethod
    def get_gfsc_jobs_with_status_product_date_tile(last_status, product_date, 
            tile_id, gfsc_job_object, logger_func, set_timeout=True):
        '''
        Get last GFSC job inserted in the database for a specified tile ID.

        :param last_status: [JobStatus]
        :param product_date: Datetime object
        :param tile_id: tile ID as a string
        :param gfsc_job_object: GfscJob()
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # Convert enum status to integer
        last_status = [enum.value for enum in last_status]

        # Get the GFSC jobs
        gfsc_jobs = StoredProcedure.call(
            response_type_instance=gfsc_job_object,
            procedure_name='gfsc_jobs_with_status_product_date_tile',
            last_status=last_status,
            product_date_ref=product_date,
            tile_id_ref=tile_id,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in gfsc_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(gfsc_jobs, parent_jobs)

        return gfsc_jobs


    @staticmethod
    def get_last_job_with_fsc_publication_latest_date(sws_wds_job_object, logger_func, set_timeout=True):
        '''
        Get last SWS/WDS job inserted in the database.

        :param sws_wds_job_object: SwsWdsJob()
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # Get the GFSC jobs
        sws_wds_jobs = StoredProcedure.call(
            response_type_instance=sws_wds_job_object,
            procedure_name='last_job_with_fsc_publication_latest_date',
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in sws_wds_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(sws_wds_jobs, parent_jobs)

        return sws_wds_jobs


    @staticmethod
    def get_last_job_with_usable_s1ass(
        assembly_id, allow_codated_jobs, sws_wds_job_object, logger_func, set_timeout=True):
        '''
        Get SWS/WDS job with most recent measurement date, inferior to the
        high time bound, with given tile id, and which didn't fail to generate
        an S1 products yet.

        :param assembly_id: assembly_id as a string
        :param sws_wds_job_object: SwsWdsJob()
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        # Get the SWS/WDS jobs
        sws_wds_jobs = StoredProcedure.call(
            response_type_instance=sws_wds_job_object,
            procedure_name='get_last_job_with_usable_s1ass',
            assembly_id=assembly_id,
            allow_codated_jobs=allow_codated_jobs,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Get the parent job IDs
        parent_job_ids = [j.fk_parent_job_id for j in sws_wds_jobs]

        # Get the parent jobs
        parent_jobs = StoredProcedure.call(
            response_type_instance=ParentJob(),
            procedure_name='parent_jobs_with_ids',
            parent_job_ids=parent_job_ids,
            set_timeout=set_timeout,
            logger_func=logger_func)

        # Join results
        StoredProcedure.join(sws_wds_jobs, parent_jobs)

        return sws_wds_jobs


    @staticmethod
    def empty_database(logger_func, set_timeout=True):
        '''
        Empty all the DataBase tables.
        
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        StoredProcedure.call(
            response_type_instance=None,
            procedure_name='empty_database',
            set_timeout=set_timeout,
            logger_func=logger_func
        )

    @staticmethod
    def empty_table(logger_func, set_timeout=True):
        '''
        Empty one table of the DataBase.
        
        :param logger_func: logger.debug or logger.info or ...
        :param set_timeout: [Boolean] to notify if a timeout should be set on the request
        '''

        StoredProcedure.call(
            response_type_instance=None,
            procedure_name='empty_table',
            set_timeout=set_timeout,
            logger_func=logger_func
        )
