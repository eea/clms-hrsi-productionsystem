# Database job

>The current document aims to present different components of the HR-S&I system under a **templatized** form. The objective is to provide our partner all the connections they will need to define (input as output) in order to plug their code to our system.

A job is the representation of an input product in the database. It follows
this product through its lifecycle in the HR-S&I system, and is used to store
revelant information.

## Required data

To create a new job we need an input product, and some information related to
this one. This way the following data are required in any job object :  

* a **unique ID** to identify the job in the database.
* a **product ID** to identify the input product.
* the **tile ID** on which the input product focus.
* the **path** leading to the  input product on your external storage.
* a **date** of publication of the input product on the external storage,
to keep track of the timeliness.
* a **date** of publication of the output product on the external storage,
to keep track of the timeliness.
* a **path** under which is stored the ouput product on the external storage.
* a **status** identifying the job progression, we currently have 16 status,
the first one which should be set at the job creation being `initialized`.
Please note that the status are not dirrectly set in the job, but in an other
table of the database named **job_status_changes**, which is linked to the
job with a specific **parent-job ID**.

## Job class

Regarding the new job class itself :

Any new job class should inherit from an already implemented `JobTemplate` class.  
The following parameters should be defined at the class level :  

* `__TABLE_NAME` : name of the database table in which will be stored the jobs.
* `JOB_NAME` : generic name of the jobs (arbitrarily set).
* `INPUT_PRODUCT_TYPE` : type of product taken as input (either `s1` or `s2`
for now).
* `NOMAD_JOB_NAME` : name of the **Nomad parametrized job** in charge of running
the job scientific software processing on a worker.
* `WORKER_FLAVOR_NAME` : size of worker the scientific software processing should
be run on (cf. [worker_flavors.py](
    ../components/common/python/database/model/job/worker_flavors.py)).
* `OUTPUT_PRODUCTS_LIST` : names of the output products in a list object (can be
a list of one element, if only on product is generated).
* `LAST_INSERTED_JOB` : instance of the current job class of the last job indexed
in the database (set to `None` by default).
* `GET_JOBS_WITH_STATUS_PROCEDURE_NAME` : name of the stored procedure used to
retrieve instances of the current job type in the database based on a provided
status (cf. [stored_procedure.py](
    ../components/common/python/database/rest/stored_procedure.py))

The **init** method of the new job class should define the parameters with a
default value set to `None`, then set them with the values passed in argument.  

## Job method

Any new type of job should have methods to interact with the database. The
required methods are listed below :

* `from_database_value` : to specify rules to read parameters of the job when
fetched from the database.
* `get_last_inserted_job` : to find the last job, of the current type, that has
been indexed in the database.
* `job_pre_insertion_setup` : to perform some actions before inserting a job in
the database, if needed.
* `configure_single_job` : to perform some actions before running the job
scientific software processing, such as handling time series for instance.
* `get_products_publication_jsons` : to set the publication JSON(s) that should be
sent to the **RabbiMq** endpoint to index the output product(s) in the external APIs
(Finder/WMS/Wekeo Portal).
* `generated_a_product` : return a boolean to notify if the job generated an
output product or not.
* `set_product_publication_date` : set the date and time at which the output
product(s) was(were) published.
* `get_jobs_to_create` : return the list of jobs that should be indexed in the
database, based on the input products found in a given time-range.
* `configure_batch_jobs` : to perform some actions on a group of jobs before
running their scientific software processing.

Currently in the HR-S&I system, all these methods are inherited from a parent
class named `JobTemplate`. This parent class **init** method is called when a job
object is instanciated, providing as argument, the `__TABLE_NAME` value.

Also note that a new job type class would imply the creation of new `stored_procedure`
methods to interact with the database, in the database sql file (cf. [init_database.sql
](../components/database/init_database.sql)).  
At least, the following method would be required (some might be missing) :

* The name of the method set in the `GET_JOBS_WITH_STATUS_PROCEDURE_NAME` parameter,
at the job class level.

# Run worker

The action of processing a job consists in running one or several scientific
scripts in dockerized environments, on a dynamically created VM. The steps to
follow to do so, are described below.

## Inputs

Several inputs are mandatory to process a job :

* the **job ID**, the number set by the database to identify the job. It will
be used to retrieve the job in the database.
* an **access to the external sotrage** , on which is stored the input product.
* the **input product path**, path leading to the input product on the external
storage.
* [optional] **access** and **path** to external dependencies if needed.

## Outputs

The job processing should end up producing a set of output as the following :

* the **return code** of the processing script, either success or failure, with
different code of failure to identify the problem root cause.
* the **processing exitcode** either **0** to notify that the system don't need
to relaunch the job processing, or **2** to retry it.
* the **path** leading to the processed product on an external storage.

## Methods

A set of methods are currently used to process jobs in the HR-S&I system, these
methods are described below, and should be re-used if one want to deploy an other
worker to process new type of jobs.

* a `job_pre_processing` method, in which the input products and the external
dependencies are copied from their sotrage into a local folder (usually `/work`)
on the worker. The aim of this function is also to organize all the copied files
and folders in the dedicated local folder on the VM, accordingly to the scientific
software requirements.
* a `job_processing` method in charge of running the scientific script under a
controlled docker environment. If the script was successfull, the generated
products should be saved in a local folder of the worker on which it was running.
* a `job_post_processing` method analyzing the return code of the scientific
software to determine if output products were generated or not. If it did, it should
also keep track of the time (under date format) at which the porducts was generated.
The method should also upload the generated product in a predefined external
storage, under an appropriate format. Finally, once all the files and folders
have been uploaded, it should clean the local folder (usually `/work`) in which
were stored the downloaded input files and uploaded ouput ones.
