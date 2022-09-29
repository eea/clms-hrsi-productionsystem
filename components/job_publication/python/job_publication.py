'''
_summary_

:raises Exception: _description_
:raises CsiInternalError: _description_
:raises CsiInternalError: _description_
:raises e: _description_
:raises CsiInternalError: _description_
:raises CsiInternalError: _description_
:raises e: _description_
:return: _description_
:rtype: _type_
'''
import json
import logging
import socket

import amqp
from kombu import Connection, Exchange, Queue
import yaml

from ...common.python.database.model.job.job_status import JobStatus
from ...common.python.database.model.job.job_types import JobTypes
from ...common.python.database.model.job.looped_job import LoopedJob
from ...common.python.database.model.job.system_parameters import SystemPrameters
from ...common.python.database.rest.stored_procedure import StoredProcedure
from ...common.python.util.exceptions import CsiInternalError
from ...common.python.util.log_util import temp_logger
from ...common.python.util.resource_util import ResourceUtil

# check if environment variable is set, exit in error if it's not
from ...common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")
SysUtil.ensure_env_var_set("CSI_PRODUCT_PUBLICATION_ENDPOINT_ID")
SysUtil.ensure_env_var_set("CSI_PRODUCT_PUBLICATION_ENDPOINT_VIRTUAL_HOST")
SysUtil.ensure_env_var_set("CSI_PRODUCT_PUBLICATION_ENDPOINT_PASSWORD")



class JobPublication(LoopedJob):
    '''
    The Job configuration module is in charge of requesting new jobs from the database
    and configuring them.
    '''

    # Path on disk of the configuration file
    __CONFIG_PATH = ResourceUtil.for_component(
        'job_publication/config/job_publication.yml')

    # Time in seconds between two requests to the database for new jobs.
    __SLEEP = None

    # Initial log level. Can be modified later by the operator for e.g. debugging jobs.
    __LOG_LEVEL = None

    # Retrieve endpoints credentials from environment variables
    rabbitmq_communication_id = SysUtil.read_env_var(
        "CSI_PRODUCT_PUBLICATION_ENDPOINT_ID")
    rabbitmq_communication_virtual_host = SysUtil.read_env_var(
        "CSI_PRODUCT_PUBLICATION_ENDPOINT_VIRTUAL_HOST")
    rabbitmq_communication_password = SysUtil.read_env_var(
        "CSI_PRODUCT_PUBLICATION_ENDPOINT_PASSWORD")

    # Establish RabbitMQ connection using Kombu library to forward json
    product_exchange = Exchange(
        'product_publication'
        'direct',
        durable=True
    )
    indexering_results = Exchange(
        'indexering_results',
        'topic',
        durable=True
    )
    json_queue = Queue(
        'json',
        exchange=product_exchange,
        routing_key='json'
    )
    json_acknowledgement_queue = Queue(
        'failed_products',
        exchange=indexering_results,
        routing_key='failed_products'
    )

    @staticmethod
    def read_config_file():
        '''Read the configuration file.'''
        with open(JobPublication.__CONFIG_PATH, 'r', encoding='UTF-8') as stream:
            contents = yaml.safe_load(stream)
            JobPublication.__SLEEP = (
                contents['sleep'])
            JobPublication.__LOG_LEVEL = (
                logging.getLevelName(contents['log_level']))

        # Overload loop's sleep value with 'system_parameters' table value
        #  if database is instanciated.
        try:
            JobPublication.__SLEEP = SystemPrameters().get(
                temp_logger.debug).job_publication_loop_sleep
        except Exception:
            pass

    @staticmethod
    def start(*args, **kwargs):
        '''Start execution in an infinite loop.'''

        LoopedJob.static_start(
            job_name='job-publication',
            job_sub_type=JobPublication,
            next_log_level=JobPublication.__LOG_LEVEL,
            loop_sleep=JobPublication.__SLEEP,
            repeat_on_fail=True)

    def looped_start(self, *args):
        '''
        Start job execution, wrapped by OtherJob.wrapped_start
        Request the database every n minutes for new jobs and configure them.
        '''

        # Use the staticmethod start()
        if not self.logger:
            raise Exception('Logger must be initialized.')

        for job_type in JobTypes.get_job_type_list(self.logger):
            self.logger.info(f'Publication service loop for {job_type.JOB_NAME} jobs')

            # Find all jobs with status processed
            jobs = job_type.get_jobs_with_last_status(job_type, JobStatus.processed, logger_func=self.logger.debug)

            # Exit if nothing was found
            if not jobs:
                self.logger.info(f'No new {job_type.JOB_NAME} jobs to publish')
                continue

            self.logger.info(f'Publish {len(jobs)} {job_type.JOB_NAME} jobs')
            for job in jobs:
                
                self.logger.info(f"    Publishing job with ID {job.id}")
                # Assert job has at least one product to publish before we try to publish it
                if job.generated_a_product():
                    # Update job status to notify that it has been handled by publication service
                    self.logger.debug(f'    Status of job with ID {job.id} set at "start_publication"')
                    job.post_new_status_change(JobStatus.start_publication)

                    # Json info to be sent to notify a product publication
                    publication_json_template = self.set_json_template()
                    self.logger.debug(f"    The json template has been set.")
                    # Get filled JSON(s) to be sent to notify product(s) publication
                    publication_json_list = job.get_products_publication_jsons(publication_json_template)
                    self.logger.debug(f"    Its publication json list is :\n{[json.dumps(publication_json) for publication_json in publication_json_list]}")

                    if not isinstance(publication_json_list, list):
                        publication_json_list = [publication_json_list]

                    # Do not send JSON if an error occurred
                    # If JSON setting failed for both product, raise an error
                    if len(publication_json_list) == 0:
                        error_subtype = "Publication JSON setting Error"
                        error_message = f"Publication service couldn't set JSON "\
                            f"content for any {' or '.join(job_type.OUTPUT_PRODUCTS_LIST)} products."

                        job.post_new_status_change(
                            JobStatus.internal_error,
                            error_subtype=error_subtype,
                            error_message=error_message
                        )
                        raise CsiInternalError(
                            error_subtype,
                            error_message
                        )

                    # For each job, publish one JSON per type of product the job generated
                    for dict_notifying_publication in publication_json_list:

                        # Find the product name
                        product = dict_notifying_publication["resto"]["properties"]["productType"]

                        # Convert product dictionary into json
                        json_notifying_publication = json.dumps(dict_notifying_publication)

                        # Send both json to rabbitMQ if RabbitMQ address has been set
                        self.send_json_to_rabbitmq(job, json_notifying_publication, product)

                        # Set products JSON publication time
                        job.set_product_publication_date(product, dict_notifying_publication)

                    # Update job content and status as at least one product has been sent
                    job.patch(patch_foreign=True, logger_func=self.logger.debug)
                    job.post_new_status_change(JobStatus.published)

                # Update job status in any case, even if no product has been published
                job.post_new_status_change(JobStatus.done)


        # Handle jobs with publication issues
        self.get_jobs_acknowledgement()


    def set_json_template(self):
        '''Create the json template to be sent to notify product publication.'''

        json_dict = {
        "collection_name": "HR-S&I",
        "resto": {
            "type": "Feature",
            "geometry": {
                "wkt": None
            },
            "properties": {
                "productIdentifier": None,
                "title": None,
                "resourceSize": None,
                "organisationName": "EEA",
                "startDate": None,
                "completionDate": None,
                "productType": None,
                "resolution": None,
                "mission": None,
                "cloudCover": None,
                "processingBaseline": None,
                "host_base": "s3.waw2-1.cloudferro.com",
                "s3_bucket": None,
                "thumbnail": None
                }
            }
        }

        return json_dict


    def send_json_to_rabbitmq(self, job, json_notifying_publication, product):
        '''
        _summary_

        :param job: _description_
        :type job: _type_
        :param json_notifying_publication: _description_
        :type json_notifying_publication: _type_
        :param product: _description_
        :type product: _type_
        :raises CsiInternalError: _description_
        :raises e: _description_
        '''
        # Retrieve address to which JSON data should sent to, from database
        rabbitmq_communication_endpoint = SystemPrameters().get(
            temp_logger.debug).rabbitmq_communication_endpoint

        if rabbitmq_communication_endpoint != "not_defined":
            rabbitmq_communication_endpoint = rabbitmq_communication_endpoint.format(
                JobPublication.rabbitmq_communication_id,
                JobPublication.rabbitmq_communication_password,
                JobPublication.rabbitmq_communication_virtual_host
            )
            # Handle special characters in full endpoint url
            rabbitmq_communication_endpoint = rabbitmq_communication_endpoint.replace('@#', '%40%23')
            rabbitmq_communication_endpoint = rabbitmq_communication_endpoint.replace(']', '%5D')
            try : 
                with Connection(
                    rabbitmq_communication_endpoint
                ) as conn:
                    producer = conn.Producer(serializer='json')
                    producer.publish(
                        json_notifying_publication,
                        exchange=JobPublication.product_exchange, 
                        routing_key='json',
                        declare=[JobPublication.json_queue]
                    )

            except amqp.exceptions.AccessRefused as error:
                # An error occured while sending request to the endpoint
                # credentials seems to be the root cause
                self.logger.error(f"Couldn't publish job with id "
                                  f"'{job.id if job is not None else None}' '{product}' "
                                  "product's JSON as RabbitMQ credentials seems to not have "
                                  f"correct values! \n  - error message : {error} \n  "
                                  f"- JSON : {json_notifying_publication}")
                # Notify the error in the job status and stop service
                if job is not None:
                    job.post_new_status_change(
                        JobStatus.internal_error,
                        error_subtype="RabbitMQ Queue Error",
                        error_message="RabbitMQ endpoint access refused, Credentials error."
                    )
                raise CsiInternalError(
                    "RabbitMQ Queue Error",
                    "RabbitMQ endpoint access refused, Credentials error."
                ) from error

            except Exception as error:
                # An error occured while sending request to the endpoint
                self.logger.error(f"Couldn't publish job with id "
                                  f"'{job.id if job is not None else None}' '{product}' "
                                  "product's JSON as RabbitMQ credentials seems to experience "
                                  f"trouble receiving our request! \n  - error message : {error} \n  "
                                  f"- JSON : {json_notifying_publication}")
                # Notify the error in the job status and stop service
                if job is not None:
                    job.post_new_status_change(
                        JobStatus.external_error,
                        error_subtype="RabbitMQ Queue Error",
                        error_message="Error while sending request to RabbitMQ queue." 
                    )
                    job.post_new_status_change(JobStatus.error_checked)
                    job.post_new_status_change(JobStatus.processed)
                raise error

        else:
            self.logger.warning("Couldn't publish job with id "
                                f"'{job.id if job is not None else None}' '{product}' "
                                f"product's JSON as RabbitMQ address is not set in "
                                f"'system_parameters' database table! \n  - JSON "
                                f"{json_notifying_publication}")


    def acknowledgement_callback(self, body, message):
        '''
        Function triggered when a message is received on the
        acknowledgement queue, meaning that a product couldn't
        be published properly on the endpoint side.
        '''

        republish_acknowledged_json = True

        # Load the acknowledgement message content : the JSON that failed
        if isinstance(body, str):
            publicated_dict = json.loads(body)
        elif isinstance(body, dict):
            publicated_dict = json.dumps(body)
            publicated_dict = json.loads(publicated_dict)

        # Retrieve the product identifier from the JSON content
        product_identifier = publicated_dict["resto"]["properties"]["productIdentifier"]
        self.logger.info(f'Product identifier is: {product_identifier}')
        if product_identifier is not None and len(product_identifier) > 3:
            # Retrieve the product name from the product identifier info
            product_identifier_list = product_identifier.split('/')
            if len(product_identifier_list) > 11:
                product_name = product_identifier.split('/')[-5]
            # Special cases for PSA/ARLIE products as they have no months/days in the path
            else:
                product_name = product_identifier.split('/')[-3]
            product_name = product_name.replace('_', '')

            # Retrieve the type of job
            returned_job_type = None
            for job_type in JobTypes.get_job_type_list(self.logger):
                if product_name.lower() in  job_type.JOB_NAME.lower():
                    returned_job_type = job_type
                    break

            if returned_job_type is None:
                error_message = (f"Couldn't find any matching job type for product "\
                    f"name : {product_name}. \n    - JSON : {publicated_dict}")
                self.logger.error(error_message)
                raise CsiInternalError(
                    "Publication Acknowledgement Error",
                    error_message
                )

            # Handle storage differences between job types
            job_output_path_parameter_name = f"{product_name.lower()}_path"
            if returned_job_type.JOB_NAME == "rlies1":
                job_output_path_parameter_name = "rlies1_product_paths_json"
                product_identifier = product_identifier.replace("/eodata/HRSI/", "")
            elif returned_job_type.JOB_NAME == "rlies1s2":
                job_output_path_parameter_name = f"{returned_job_type.JOB_NAME.lower()}_path"
                product_identifier = product_identifier.replace("/eodata/HRSI/", "")
            elif returned_job_type.JOB_NAME == "PSA/ARLIE":
                job_output_path_parameter_name = "result_path"

            # Find the job which failed from the database
            job = returned_job_type().attribute_like(job_output_path_parameter_name, 
                f"%{product_identifier}%", case_sensitive=False).get(self.logger.debug)
            # The request response is a list -> return the first and unique element
            #  if it exists, else return None
            if isinstance(job, list) and len(job) > 0:
                job = job[0]
            else:
                self.logger.warning("Product with identifier '%s' failed to be "\
                "publish on endpoint side, but we couldn't retrieve a job in the "\
                "database with a corresponding product path! \n  -full JSON : \n%s" %(
                    product_identifier,publicated_dict))
                job = None

            if job is not None:
                self.logger.warning("Job '%s' failed to be published on the endpoint side." %job.id)

                # Get the job status history
                status_history = StoredProcedure.job_status_history(
                    [job.fk_parent_job_id],
                    logger_func=self.logger.debug
                )

                # Count the number of times the job failed to be published
                publication_external_error_cpt = 0
                for i in range(len(status_history)):
                    if (
                        status_history[i].status == JobStatus.external_error
                        and i > 0
                        and status_history[i-1].status == JobStatus.done
                    ):
                        publication_external_error_cpt += 1

                # If the job already failed to be published 3 times, 
                # raise an internal error.
                if publication_external_error_cpt >= 3:
                    job.post_new_status_change(
                        JobStatus.internal_error,
                        error_subtype="Endpoint Publication Failure",
                        error_message=f"'{product_name.upper()}' product publication "\
                        "failed on the endpoint side 3 times in a row. There must "\
                        "be an issue with the job publication service."
                    )
                    republish_acknowledged_json = False

            if republish_acknowledged_json:
                self.logger.warning("Republishing mentioned job or single JSON, "\
                    "as it has been triggered by the acknowledgement queue !")

                # Find the product name
                product = publicated_dict["resto"]["properties"]["productType"]

                # Convert product dictionary into json
                json_notifying_publication = json.dumps(publicated_dict)

                # Send both json to rabbitMQ if RabbitMQ address has been set
                self.send_json_to_rabbitmq(job, json_notifying_publication, product)

                if job is not None:
                    # Update job status to notify that we republished it
                    job.post_new_status_change(
                        JobStatus.external_error,
                        error_subtype="Endpoint Publication Failure",
                        error_message=f"'{product_name.upper()}' product publication failed "\
                        "on the endpoint side. We can try to publish the product again."
                    )
                    job.post_new_status_change(
                        JobStatus.error_checked,
                        error_message="Error automatically checked as it's most likely "\
                        "coming from the endpoint side."
                    )
                    job.post_new_status_change(JobStatus.done)

        message.ack()


    def get_jobs_acknowledgement(self):
        '''
        Retrieve JSONs that encountered errors during their
        publication process, on the endpoint side.
        '''

        # Retrieve address from which JSON data should be listened from
        rabbitmq_communication_endpoint = SystemPrameters().get(
            temp_logger.debug).rabbitmq_communication_endpoint

        if rabbitmq_communication_endpoint != "not_defined":
            self.logger.info("Reading RabbitMQ acknowledgement queue...")
            rabbitmq_communication_endpoint = rabbitmq_communication_endpoint.format(
                JobPublication.rabbitmq_communication_id,
                JobPublication.rabbitmq_communication_password,
                JobPublication.rabbitmq_communication_virtual_host
            )
            # Handle special characters in full endpoint url
            rabbitmq_communication_endpoint = rabbitmq_communication_endpoint.replace('@#', '%40%23')
            rabbitmq_communication_endpoint = rabbitmq_communication_endpoint.replace(']', '%5D')
            try :
                with Connection(
                    rabbitmq_communication_endpoint
                ) as conn:
                    with conn.Consumer(
                        JobPublication.json_acknowledgement_queue,
                        callbacks=[self.acknowledgement_callback],
                        accept=['json']
                    ) as _:
                        # Process messages and handle events on all channels,
                        # automatically raise a timeout after 1 sec
                        while True:
                            conn.drain_events(timeout=1)

            except socket.timeout:
                pass

            except amqp.exceptions.AccessRefused as error:
                # An error occured while trying to connect to the endpoint,
                # credentials seems to be the root cause
                self.logger.error("Couldn't connect to the acknowledgement "
                                  "queue as RabbitMQ credentials seems to not have "
                                  f"correct values! \nerror message : {error}")
                # Notify the error and stop service
                raise CsiInternalError(
                    "RabbitMQ Queue Error",
                    "RabbitMQ endpoint access refused, Credentials error."
                ) from error

            except Exception as error:
                # An error occured while sending request to the endpoint
                self.logger.error("Couldn't connect to the acknowledgement "
                                  "queue as RabbitMQ endpoint seems to experience "
                                  f"trouble! \nerror message : {error}")
                # Notify the error and stop service
                raise error

        else:
            self.logger.warning("Couldn't connect to the acknowledgement "\
                "queue as RabbitMQ address is not set in "\
                "'system_parameters' database table!")

# Static call: read the configuration file
JobPublication.read_config_file()
