import os
import re
import json
from datetime import datetime

from .job_template import JobTemplate
from .worker_flavors import WorkerFlavors
from ....util.fsc_rlie_job_util import FscRlieJobUtil


class PsaArlieJob(JobTemplate):
    '''
    Description of a job for the generation of one PSA product from accumulated FSC products.
    
    :param psa_arlie_id: PSA/ARLIE product ID.
    :param request_id: job creation request ID.
    :param product_type: product type (PSA/ARLIE).
    :param reprocessing: job priority level.
    :param hydro_year: (datetime) start date of the hydrological year for PSA product.
    :param month: (datetime) month over which RLIE porduct were generated to produce ARLIE result.
    :param first_product_measurement_date: (datetime) measurement date of first FSC product used.
    :param last_product_measurement_date: (datetime) measurement date of last FSC product used.
    :param input_path: (dictionnary) FSC/RLIE file paths on DIAS sort by tile.
    :param result_infos: (json) information relative to PSA/ARLIE product.
    :param result_path: PSA/ARLIE file path on DIAS.
    :param result_completion_date: (datetime) PSA/ARLIE publication date in the bucket.
    :param result_json_publication_date: (datetime) PSA/ARLIE JSON publication date on RabbitMQ endpoint.
    '''
    
    # Database table name
    TABLE_NAME = "psa_arlie_jobs"

    # Class job name
    JOB_NAME = "PSA/ARLIE"

    # Sentinel input product type
    INPUT_PRODUCT_TYPE = "s2"

    # Name of the Nomad job processing the present job type
    NOMAD_JOB_NAME = "si-processing"

    # Worker flavor required for the present job type processing
    WORKER_FLAVOR_NAME = WorkerFlavors.medium.value

    # Name of the products the job will generate during it's processing
    OUTPUT_PRODUCTS_LIST = ["psa", "arlie"]

    # Name of the stored procedure used to retrieve TestJob with a given status
    GET_JOBS_WITH_STATUS_PROCEDURE_NAME = "psa_arlie_jobs_with_last_status"

    def __init__(self, **kwds):
        self.psa_arlie_id = None
        self.request_id = None
        self.product_type = None
        self.reprocessing = None
        self.hydro_year = None
        self.month = None
        self.first_product_measurement_date = None
        self.last_product_measurement_date = None
        self.input_paths = None
        self.result_infos = None
        self.result_path = None
        self.result_completion_date = None
        self.result_json_publication_date = None
        
        # Call the parent constructor AFTER all the attributes are initialized with None
        super().__init__(PsaArlieJob.TABLE_NAME)
        
        # Attribute values given by the caller
        for key, value in kwds.items():
            setattr(self, key, value)

    # Build a string for PostgREST query parameters
    
    def attributes_eq(self, values_dict):
        return super().attributes_eq(values_dict)


    def configure_job(self):
        '''Configure new PSA/ARLIE jobs.'''

        """ # Find all jobs with status initialized
        jobs = StoredProcedure.psa_arlie_jobs_with_last_status(
            JobStatus.initialized, PsaArlieJob(), logger_func=self.logger.debug)

        # Exit if no jobs
        if not jobs:
            self.logger.info('No new jobs to configure')
            return

        self.logger.info('Configure %d PSA jobs' % len(jobs))

        # Update the PSA/ARLIE jobs with information coming from the completed FSC/RLIE ones
        self.get_product_results_data(jobs, logger_func=self.logger.debug)

        # Update the job priority levels
        self.update_priority(jobs)

        for job in jobs:

            # Set job name from the tile ID and date
            if job.product_type in ["PSA-WGS84", "PSA-LAEA"]:
                job.name = 'psa %s' % job.tile_id
                job.name += ' %s' % job.hydro_year
            else:
                job.name = 'arlie %s' % job.tile_id
                job.name += ' %s' % job.month

            # Update the job in the database
            job.patch(patch_foreign=True, logger_func=self.logger.debug)

            # Set status to pending
            job.post_new_status_change(JobStatus.configured)
            job.post_new_status_change(JobStatus.ready) """

        raise NotImplementedError


    def get_product_results_data(self, psa_arlie_jobs, logger_func=None):
        '''Loop over completed FSC/RLIE jobs to retreive data needed by each PSA/ARLIE jobs'''

        """ for psa_arlie_job in psa_arlie_jobs:
            # Instanciate lists in which will be stored the FSC/RLIE jobs data
            measurement_dates = []
            fsc_rlie_paths = []

            # Set lower and upper time bound for FSC/RLIE job selection
            if psa_arlie_job.product_type in ["PSA-WGS84", "PSA-LAEA"]:
                ''' 
                 PSA job covers one hydrological year running from the first day of May,
                  until the last day of September.
                   - so we take as an inclusive lower bound the first day of the 5th month (May).
                   - and we take as an exclusive upper bound the first day of the month following 
                       September, which is October the 10th month (9+1).
                '''
                selected_year = datetime.strptime(psa_arlie_job.hydro_year, '%Y-%m-%dT%H:%M:%S').year
                low_time_bound = datetime(selected_year, 5, 1).strftime('%Y-%m-%d')
                high_time_bound = datetime(selected_year, 9+1, 1).strftime('%Y-%m-%d')

            else:
                ''' 
                 ARLIE job covers one month of a year.
                   - so we take as an inclusive lower bound the first day of the month.
                   - and we take as an exclusive upper bound the first day of the next month.
                '''
                date_object = datetime.strptime(psa_arlie_job.month, '%Y-%m-%dT%H:%M:%S')
                selected_year = date_object.year
                selected_month = date_object.month
                low_time_bound = datetime(selected_year, selected_month, 1).strftime('%Y-%m-%d')
                high_time_bound = datetime(selected_year, selected_month+1, 1).strftime('%Y-%m-%d')

            # Retreive completed FSC and RLIE jobs with selected measurement time and tile
            # TODO MUST : select only jobs which produced FSC/RLIE products
            fsc_rlie_jobs = StoredProcedure.fsc_rlie_jobs_with_status_tile_date(
                JobStatus.done, 
                psa_arlie_job.tile_id, 
                low_time_bound, 
                high_time_bound,
                FscRlieJob(),
                logger_func=self.logger.debug
                )

            # Sort jobs by measurement date
            fsc_rlie_jobs.sort(key=lambda job: job.measurement_date)

            # Get suitable completed FSC and RLIE jobs data
            for fsc_rlie_job in fsc_rlie_jobs:
                if (
                    psa_arlie_job.product_type in ["PSA-WGS84", "PSA-LAEA"] 
                    and fsc_rlie_job.fsc_path is not None
                    or psa_arlie_job.product_type == "ARLIE" 
                    and fsc_rlie_job.rlie_path is not None
                   ):

                    measurement_dates.append(fsc_rlie_job.measurement_date)
                    if psa_arlie_job.product_type in ["PSA-WGS84", "PSA-LAEA"]:
                        fsc_rlie_paths.append(fsc_rlie_job.fsc_path)
                    else:
                        fsc_rlie_paths.append(fsc_rlie_job.rlie_path)
                else:
                    self.logger.error("An FSC/RLIE job was found with a result path set to None")
                    raise CsiInternalError(
                    "Job Configuration Error",
                    "Couldn't retrieve FSC/RLIE job result path."
                    )

            # Find first/last products measurement dates
            first_product_measurement_date = None
            last_product_measurement_date = None

            if len(measurement_dates) > 0:
                first_product_measurement_date = min(measurement_dates)
                last_product_measurement_date = max(measurement_dates)

            # Update PSA/ARLIE jobs with data from suitable FSC/RLIE jobs
            psa_arlie_job.first_product_measurement_date = first_product_measurement_date
            psa_arlie_job.last_product_measurement_date = last_product_measurement_date
            psa_arlie_job.input_paths = ";".join(fsc_rlie_paths) """

        raise NotImplementedError


    def set_product_publication_date(self, product_name: str, publication_json: dict = None):
        '''
        Set the publication date of a given type of product.
        
        :param product_name: name of the product to update the publication date.
        '''

        self.result_json_publication_date = datetime.utcnow()
        if publication_json is not None:
            self.result_infos = publication_json


    def get_products_publication_jsons(self, publication_json_template):
        '''
        Fill the json to be sent to notify product publication.

        :param publication_json_template: JSON template to be filled before sending.
        '''

        dict_notifying_publication = publication_json_template

        # Json info set by SI_software/worker
        json_set_by_worker = None

        # If product info were set by the worker, load them
        if self.result_infos is not None:
            json_set_by_worker = json.loads(self.result_infos)

        # Retrieve parameters values
        (s3_bucket, object_path) = FscRlieJobUtil.split_bucket_and_object_from_path(
                self.result_path, prefix=True)
        product_identifier = self.result_path
        product_type = self.product_type.upper().replace("-", "_")
        thumbnail = os.path.join(
                FscRlieJobUtil.get_quicklook_bucket_path(object_path), 
                "thumbnail.png"
            )

        # Product info were not set by worker -> raise a warning
        if json_set_by_worker is None:
            self.logger.warning("Couldn't publish job with id '%s' '%s' "\
                "product's JSON as no information were set by the worker!"\
                %(self.id, self.product_type))
            return 1

        # Retrieve information set by worker/SI_software
        try:
            # Retrieve geometry information
            dict_notifying_publication[
                "resto"][
                    "geometry"][
                        "wkt"] = json_set_by_worker[
                                    "resto"][
                                        "geometry"][
                                            "wkt"]
                                            
            # Retrieve resource size information, provided in Kb, convert it in Bytes
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "resourceSize"] = 1024 * int(json_set_by_worker[
                                    "resto"][
                                        "properties"][
                                            "resourceSize"])
                                            
            # Retrieve resolution information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "resolution"] = json_set_by_worker[
                                    "resto"][
                                        "properties"][
                                            "resolution"]

        except KeyError:
            self.logger.error("Couldn't publish job with id '%s' '%s' "\
                "product's JSON as information set by worker are not "\
                "relevant!" %(self.id, self.product_type))
            return 1

        # Fill json to be sent
        try:
            # Add the current product collection name
            dict_notifying_publication[
                "collection_name"] = "HR-S&I"

            # Add type
            dict_notifying_publication["resto"]["type"] = "Feature"

            # Add product identifier information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "productIdentifier"] = product_identifier

            # Add product title information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "title"] = os.path.basename(os.path.normpath(
                            product_identifier))

            # Add product organistation name information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "organisationName"] = "EEA"

            # Add product start date information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "startDate"] = self.first_product_measurement_date.strftime(
                            '%Y-%m-%dT%H:%M:%S.%f')

            # Add product completion date information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "completionDate"] = self.result_completion_date.strftime(
                            '%Y-%m-%dT%H:%M:%S.%f')

            # Add product type information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "productType"] = product_type

            # Add system's version information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "processingBaseline"] = re.search(
                            r'V\d([\d]+)', product_identifier).group(0)

            # Add product host base information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "host_base"] = "s3.waw2-1.cloudferro.com"

            # Add product bucket information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "s3_bucket"] = s3_bucket

            # Add product thumbnail (quicklook) information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "thumbnail"] = thumbnail

            # Add mission information
            dict_notifying_publication[
                "resto"][
                    "properties"][
                        "mission"] = PsaArlieJob.INPUT_PRODUCT_TYPE.upper()

        except Exception as e:
            self.logger.error("Couldn't publish job with id '%s' '%s' "\
                "product's JSON as the following error occured during "\
                "its setting : \n%s" %(self.id, self.product_type, e))
            return 1

        return 0