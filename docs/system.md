# Technologies

>This document contains the **description of the HR-S&I system internals**  
The SSD document (from the HR-S&I project official documentation) provides a
high level overview of the system and of its functional architecture. It might
also contain some lower level information. Please make sure you are familiar
with it before digging in the present document.

The present document gives lower level information, describes some technical
choices and helps understand how things actually work.

## Infrastructure

The HR-S&I system is deployed on a cloud platform that is managed by
**OpenStack**, which is the only external dependency. For now, the cloud
provider is **Wekeo**, but only few parts explicitly rely on Wekeo
(Input data access still relies on CloudFerro, the previous cloud provider used).

Most of the infrastructure is defined and actually managed using **Terraform**.

In rare cases some resources might have been created or are managed manually
with the **Horizon** dashboard (Horizon is the name of the OpenStack dashboard).

Some **dynamic** resource management (to scale up and down the workers, for
example) don't use Terraform but directly use OpenStack.

See [infra.md](infra.md) for more information about the infrastructure.

## Database

The database is based on **PostgreSQL** and the HTTP API is provided by
**PostgREST**. For now they are both hosted on the same VM, but it might be
better to not host PostgREST alongside the database for performance and security
reasons.

## Containers and service/batch management

When possible, softwares are packaged in **docker** images. It is the case for
all **orchestrator** services and for the **scientific softwares** (FSC/RLIE,
SWS/WDS, GFSC, ...).

We have chosen to not run the database in a docker container because it is a
critical part of the system and it is seen as more robust to run it directly in
the VM.

The **worker** software is not packaged in a docker image because it needs
itself to run a docker container for scientific softwares which is complicated
to do from another container.

The orchestrator **service** containers are managed by **Nomad**
which is a "A simple and flexible workload orchestrator to deploy and manage
containers and non-containerized applications across on-prem and clouds at
scale."

**Nomad** is also used as a batch **queue** management and placement for for the
execution of the **processing** jobs.

# Description of the system by component

## Database

The database is mainly used to store the **jobs** informations. A job can be a
S&I processing job (FSC/RLIE, SWS/WDS, GFSC, ..., most of the time) but the
orchestrator services are also registered in the database as jobs. There is
a `parent_job` table (for common columns) and some specialized table for each
job subtype.

During their life cycle, jobs will have different status. And this is stored in
the database as **status changes**. The different status are (see
[init_database.sql](../components/database/init_database.sql)):

* `initialized`
* `configured`
* `ready`
* `queued`
* `started`
* `pre_processing`
* `processing`
* `post_processing`
* `processed`
* `start_publication`
* `published`
* `done`
* `internal_error`
* `external_error`
* `error_checked`
* `cancelled`

The change of status follows some **constraints** at the SQL level (i.e. we
can't change from status `configured` to `processed`). All status changes are
recorded with their **date** and some optional subtype and **message**.

Some **system parameters** are also stored in the database (like the scientific
softwares docker images names and tags for example) so the different components
can access them during runtime (like the worker for the scientific softwares
docker images). An history table records the changes on this table (with their
dates).

Each job has a **unique id** (actually an UUID) to be used when a true universal
ID is needed, like to determine some path for the storage of some files
associated to the job in a bucket.

The database still contains an **obsolete** and unused **log management**. It
has been unplugged because it uses to much space and it is not safe to keep it
in the same database instance as the jobs. This should be deleted in the future
or used in some other database instance if it is found useful.

The database contains several **SQL views** definitions dedicated to the
**dashboard**, which needs some synthetic view on the S&I jobs tables.

## HTTP API

The HTTP API is the only access to the database content. All parts of the system
use this HTTP API which is a **central** communication channel. It is provided
by PostgREST which maps URL for getting and modifying data to the actual SQL
requests. See the PostgREST documentation for the syntax of these HTTP requests.

There is some common **Python** module that encapsulates the calls to PostgREST
HTTP API. See [components/common](../components/common).

The **dashboard** has a small need to the HTTP API (mainly for the S&I jobs
views) and doesn't use some special JavaScript module for this, the URL are
simply formatted with template strings.

The rare **bash** scripts that need to access the HTTP API use basic curl
request.

As this API is used by the system inside an internal network, there is **no
authentication** to access the HTTP API.

## Orchestrator main services

The orchestrator services are developed in Python and interact with the database
using the HTTP API.

### Job creation

This service request the DIAS and the HR-S&I APIs for new input products every
5 seconds. It checks if the input product already exists in the **database**
and if it doesn't, the service create a new entry in the **database**, a **job**
which keep track of the input product at the system level.

### Job configuration

The **job configuration** service is in charge of retrieving, computing essential
information for the job processing, and store them in it.  
For instance it :  

* Requests external APIs for additional information if needed (ESA SciHub for
  instance)
* Determines the priority of a job, based on its measurement and
publication dates.
* Compute if a job is eligible to be processed based on the time-serie (if the
job is sbject to time-serie). basically, a job can be processed if the previous
one in the time-serie has already been processed.

### Job execution

This service affect a Nomad allocation to a given job, which went through the
**creation** and the **configuration** procedure, to process it on a dynamically
created VM.

To do so, upon receiving a new job needing to be processed the **job execution**
service ask Nomad to create a new dispatch based on the associated template
(`si-processing` for FSC/RLIE jobs for instance, defined in the
`si_processing.nomad` file), on an available worker, passing the job `ID` and
`unique ID` as arguments.  
The two tasks `run-worker` and `upload-nomad-alloc-dir` defined in the Nomad
file (`si_processing.nomad` in this example) are then run on the selected worker.  

The first task, `run-worker`, is in charge of retrieving the **scientific
software** docker image (`si_software` for instance), ensuring that this one
is available, and calling the python script in charge of processing the job
(run_XXX_worker.py), passing as argument the job ID.  
Inside the `run_XXX_worker.py` script, we call a docker command to run the
**scientific software**. Then, we analyze the code returned by the command, to
determine if products were generated, or if an error occurred, and adequately
set the python script exitcode.  
Depending on the exitcode returned by the `run_XXX_worker.py` script, it
notifies Nomad if the allocation should be relaunched (exitcode = 2), or not
(exitcode = 0). This exitcode is set by default to zero, and its value can
change depending on the type of errors that might be caught or raised during
the job processing.  

The second one, `upload-nomad-alloc-dir`, regularly upload in the `hidden_value`
bucket the Nomad allocation logs.

### Job publication

The **job_publication** service monitors the availability of jobs which status is
`processed` and check if the job has produced some output products.

If it is the case, the service gathers some information about the job from the
database and **send a publication** request to the **RabbitMQ** endpoint from
Wekeo.

This request uses a list of **endpoints URL** that are stored in the
`system_parameters` table of the database and the **authentication**
informations come from environment variables:
`CSI_PRODUCT_PUBLICATION_ENDPOINT_ID` and
`CSI_PRODUCT_PUBLICATION_ENDPOINT_VIRTUAL_HOST` that are defined in `main.env`
and `CSI_PRODUCT_PUBLICATION_ENDPOINT_PASSWORD` which is defined during
deployment (so that it is not stored in any file that is managed by git).

If there was a product to publish, the **job status** is set to
`start_publication` then to `published`. In any case, when the service has
finished managing the job, the status of the job is set to `done`.

Upon recieving a publication JSON, the endpoint is then able to index the
product that we generated, and stored on S3 bucket, into the Finder and WMS
catalogues.

### Monitor

The monitor service ensure that none of the jobs being executed are encountering
trouble with Nomad. If they do it will automatically put them back in the queue
to get a new Nomad allocation and be processed again.

To do so, it gather all the jobs in **"processing"** status range (`started`,
`pre_processing`, `processing`, `post_processing`) at a given time. Then, it
checks their Nomad status. If anything suspicious which could indicate that
the link between the allocation actually running the **processing** and the
Nomad server might be broken, it will stop the allocation, and put the job
back in the queue to start its processing again in a new allocation.

In addition, it ensure that the jobs won't spend too much time in the
**"processing"** status (`started`, `pre_processing`, `processing`,
`post_processing`). Indeed, a threshold value is set for each of these status,
so that if a job stay longer than the maximum expected time in one of the
status listed above, the `Monitor` service stops the job allocation, and put
the it back in the queue to start its processing again in a new allocation.

### Worker pool

This service is descirbed in the next section.

## Worker pool

### Presentation

This component **manages** the pool of workers which are the **VM instances**
needed to execute the S&I processing **jobs**.

Every day, up to **few hundreds** of workers are created and destroyed. The max
number of allowed workers is defined in the `system_parameters` table in the
database (210 at the beginning of the operational phase).

A **worker**, is an OpenStack **VM instance** with a preconfigured system, that
runs a Nomad agent (or **Nomad node**). The Nomad node is connected to the Nomad
server and advertises itself as able to run any software processing Nomad jobs
(named as : `SOFWARE_NAME-processing`, `si-processing` for instance). The
**Nomad server** will then ask the worker to execute these jobs
when new jobs are added to its **queue** by the `job execution` service.

So, at a given time, there are:

* A set of worker **OpenStack instances**.
* A set of worker OpenStack instances that have been **registered** as **Nomad
  node**.
* A set of worker Nomad nodes that are actually **running a job**.
* A set of worker Nomad nodes that are **sleeping**.
* A list of **pending** Nomad **jobs** in the Nomad queue.

The **objective** of the worker pool is to constantly **monitor** all of this to
**decide** if:

* There is a need for the **creation** of new worker. This happens when all
  workers are busy and there are some jobs in the Nomad queue.
* There is a need for the **deletion** of some workers. This is needed if the
  Nomad job queue is empty and there are some sleeping workers.
* There are some workers in **stale state** that need to be **deleted**. A stale
  worker is an OpenStack worker instance that as been created some time ago (say
  15 minutes) that is not known by Nomad. Generally, this means that something
  went wrong during the creation of the worker.

When needed, the worker pool **asks** directly **OpenStack** to create/delete
worker instances.

Note that each scientific software has different requirements in terms of
ressources (VCPUs and RAM). Therefore, to limit the computation costs we use
different worker flavors (different worker size in terms of VCPUs and RAM).
This implies that the **Worker pool service** must be aware of the types of
workers that are available, and the types of workers that are needed at any time.

As this component is developed with Python, we use the OpenStack Python SDK for
interaction with the OpenStack endpoints. The interaction with the Nomad server
uses the Nomad Python SDK.

### Scaling strategy

Some strategy are used to try to **smoothly** up or down **scale** the number of
**workers**, with the priority of having all the workers that are needed as fast
as possible and trying to not overload the OpenStack server that manages the
creation/deletion requests. Here are some choices for that:

* We don't create too much workers at a time.
* We to delete only one worker at a time and only if there is no need to create
  some.

As mentioned in the previous section, each scientific software is designed to
run on a specific worker flavor. Therefore, to scale up/down efficiently, the
**Worker pool service** must be aware of the types of workers that are available,
and the types of workers that are needed at any time.

### The tricky part: worker deletion

The **deletion** of a worker is very **tricky**. Here is why and how we manage
this:

* When the worker pool decides to delete a **sleeping worker** we must be
  careful because between the moment we analyze the situation and the time we
  want to ask OpenStack to delete the worker, Nomad **might have allocated a new
  job** to the worker.
* **If** we **naively** ask OpenStack to **delete** the worker, the
  corresponding job will be in a "running" kind of status but it will never
  be able to complete it's processing. It will then need to wait for the
  **Monitor service** to detect it and to put it back in the processing queue.
* That's why, when the worker pool chooses to delete a worker, he first ask
  **Nomad** to switch the corresponding worker to **ineligible** so that **no
  new job** will be allocated to the worker.
* **But** even with that, there is **another risk** of deleting a worker with a
  **running job**. In fact, it is possible that a new job has been allocated
  **just before** the **ineligibility** switch.
* So we also need to **check** if there are **no running** job on a worker
  **before** actually **deleting** it with OpenStack.
* **If** there is a **running job** on an ineligible Nomad worker node we only
  have to **wait** the job is finished, and **then** we can safely ask OpenStack
  to **delete** the worker instance for this worker.

### Creation of a new worker

This section gives information about how the worker pool manages the creation of
new workers.

* At the deployment of the system a **template instance** is created:
  `tf-titi`. This instance use the smallest possible flavor (like a
  `eo1.xsmall` in the CloudFerro platform).
* It **contains** all what is **needed** for a worker to start: worker software
  package, docker, Nomad, etc.
* A **Nomad** agent is configured but is **not started** (so that it doesn't
  register itself to the Nomad server) but set to start on the next boot.
* The idea is to **duplicate** this instance when the worker pool needs to
  create a new worker.
* For that, we need to create the new worker with a **disk image** that is build
  from the **template**. OpenStack allows to do this by first creating the VM
  image from `tf-titi` then using this image for the new worker
  instance creation.
* To not have to create the VM image on each worker creation, the image is
  **saved** using a **name** that contains the `tf-titi` **unique
  ID**. For example if this unique ID is `68b034ac-3206-4779-8a7b-1033bfbe6f20`)
  the image name is `os-worker-template-68b034ac`.
* If the **template** is replaced with a **new version**, when the worker pool
  needs to create a new worker, it detects that its unique ID has changed, it
  **deletes** the **old image** and **creates** a **new** one from the new
  instance. And finally creates a new worker instance from this new image.

## Nomad

### Presentation

The official presentation describes Nomad as "A simple and flexible workload
orchestrator to deploy and manage containers and non-containerized applications
across on-prem and clouds at scale."

In short, here is how Nomad works (for more details, see the Nomad official
documentation):

* There is the **server** part which centralize everything and takes the
  decisions.
* There are the Nomad **clients**, that are machines where the server can
  allocate some tasks/jobs.
* There are the **Nomad tasks/jobs** which are the thing we want to actually
  run.
* There are the **allocations**, in the sense that the server choose to allocate
  a task/job to an available client.

### Our implementation

For our system, we use Nomad for two things:

* Manage **orchestrator** services and be sure they are always up.
* Manage the processing **jobs** queue and execution.

In more details, we have:

* A Nomad **server** called `tf-tete`. The documentation **recommends**
  to have **three servers** to be to be sure there is always a server that is
  running and to not have loss of data. In **our case**, we consider that this
  is **not a critical part** of the system: all our important information is on
  our database. If the server crashes (which might be rare) we can restart with
  a new one and the system will work again (maybe, on some occasions a
  maintenance task on some jobs that were stuck in processing state will be needed).
  We **choose** to have only **one server**. But this can be changed easily if it
  is seen as important.
* Some Nomad service **jobs** for running the **orchestrator services**. These
  are **docker** Nomad jobs. The docker images are hosted in the Magellium
  GitLab registry of the project.
* Some Nomad batch **jobs** for **S&I processings**. The type of these jobs is
  **`raw_exec`**, which means there are ran as a simple command execution with
  no sandboxing. We don't use a docker job because these jobs need themselves to
  run a docker container, which is not simple to do from another docker
  execution.
* A VM instance named **`tf-tata`** which is a Nomad **client**
  dedicated to hosting all the **orchestrator** services.
* A variable number of **workers** Nomad **client** which are some VM instances
  named **`os-worker-###`** (where `###` is the number of the instance between 1
  and the max number of workers) dedicated to running the Nomad batch jobs for
  S&I processings. Note that workers can have different flavors (different sizes
  in terms of VCPUs and RAM)

### Nomad logs

The most recent part of the jobs logs can be viewed from the Nomad **dashboard**.
The log are also **stored** in the `/opt/nomad` **directory** of the client.
There is a directory for each **allocation** (which name is the allocation ID).

Nomad don't **keep** all the logs indefinitely to preserve disk space. There are
two parameters to **control** that: the number of log files (default is 10) and
their size (default is 10 MB). Those parameters are controlled at the job level.

So, to **investigate** some not so recent log for a given orchestrator job, one has to connect to the client (i.e. `tf-tata`) then go to the `/opt/nomad` directory, find and go into the log directory for the allocation (the alloc ID can be found in the Nomad dashboard). Example:

```shell
$ cd /opt/nomad/alloc/5ef2fbd2-4a23-3566-1c1a-5996db90830e/alloc/logs
$ ls -l
total 14640
-rw-r--r-- 1 root root     5796 Jun 27 12:37 job-creation.stderr.0
-rw-r--r-- 1 root root 10485506 Jun 27 05:14 job-creation.stdout.0
-rw-r--r-- 1 root root  4487454 Jun 27 14:16 job-creation.stdout.1
```

The number is continually increased when the size limit is hit, and Nomad will
delete old log files when the file number limit is hit.

We have choose to **save** the **S&I processings** jobs logs in (near) real
time. For, that there is a second task in the job definition that regularly
upload the log directory for the allocation in the **`f-sip-results` bucket**
with a path based on the CSI job unique ID. In particular, this allow the
dashboard to access the logs of a running job.

# A FSC/RLIE job workflow

Here is the description of the life cycle of a job and what is happening in the
system.

## Job preparation

First there is a **L1C monitoring task**, which asks the DIAS HTTP API for new
availability of L1C products since the last L1C product that has been added to
the database. This monitoring is done by the `job_creation` service which
**creates** a new **job** for each new L1C and set its status to `initialized`

The `job_configuration` service set some **information** needed for the
processing (some informations are retrieved from the SciHub HTTP API) and if it
is **ready** for execution it changes its status to `ready`. If it is not ready
the status is set to `configured`.

The `job_execution` service monitor the availability of jobs which status is
`ready` and submitted them to the **queue** managed by Nomad. The status is then
set to `queued`.

## L1C processing jobs management with Nomad

Nomad is the tool that manage the queuing and allocation to workers of the S&I
processing jobs. For that a parametrized batch **Nomad job** has been defined.

For Nomad, a **batch** job is a job that is ran once as opposed to service Nomad
jobs that must always be alive. For Nomad a **parametrized** job, is a job that
takes some parameters and that is not run when it is submitted to Nomad (even if
this submission is done with the `nomad run my_job.nomad` command). The job is
actually ran with the **dispatch** command which get the values for the
parameters.

**Important**. The same term "job" is used in different contexts. First, there
are the **CSI jobs**, i.e. jobs that have an entry in our database. This kind of
job is used to manipulate our FSC/RLIE processing jobs and orchestrator service
jobs. Then, there are also **Nomad jobs** which can be related to **CSI jobs**
but covers a different reality and management. At last, there is the **Nomad job
dispatch** that corresponds to a demand of execution of a parametrized batch
Nomad job.

So, what **`job_execution` service** is doing is to send to Nomad a **dispatch**
request with the parameters values (i.e. essentially the CSI job ID) for the
`si-processing` batch Nomad job. If the dispatch succeeds, its ID is stored in
the CSI job in the database for further reference.

At this point, the life of the job execution is managed by Nomad. The job is
added to the queue and will be **allocated** to a worker when one is available.
Nomad ensures that the job is **relaunched** in case of **error**. The number of
time Nomad retries to launch a job depends on the Nomad job configuration. So
the key point here is to have our jobs exiting on error only for cases where we
think a relaunch may be successful. For example, we consider this is the case of
errors on access to "external" resources (like input L1C files stored in EODATA
bucket or files stored on our internal buckets), on this errors we exit with a
non zero code so that Nomad can try it again.

When the Nomad job is run on a worker, the CSI worker software is executed.

## Running the job in a worker

The management of the actual run of a FSC/RLIE job is done by the component
[components/worker](../components/worker). It is mainly a Python component that
interacts with the database.

The running of the job follows several **steps** (which correspond to status
changes):

* `started` at the very **beginning** of the run.
* `pre_processing` which gets input and auxiliary data from **buckets** and
  **prepares** things for the S&I software. It also set a **local working
  directory** which name uses the UUID of the job and creates the parameters
  file with the appropriate values.
* `processing` which actually executes the **docker run** of the S&I software
  with the parameter file and in the working directory for the job.
* `post_processing` which analyzes the **S&I software exit status** and
  **outputs**, and uploads files on buckets if needed. These files are: the
  FSC/RLIE products generated by the S&I software, the L2A metadata and the
  working directory. If there was an unexpected error, all the working directory
  (without L1C products) is uploaded for further offline investigation. If there
  was no error, only the S&I software `output` directory is uploaded (which
  contains the logs of all S&I software components). The **job** is **updated**
  in the **database** with new informations like the path to the FSC/RLIE
  products in the product bucket, the products completion dates and some other
  informations provides by the S&I software.
* `processed` if everything is **OK** and we exit with **status code 0**.
* `internal_error` if there is an error that is **unrecoverable**, i.e. that we
  know for sure that if we relaunch the job this error will happen again. In
  that case we also exit with **status code 0**, so that **Nomad** will **not
  try** to execute the job again.
* `external_error` if there is an error that is due to some kind of **external
  reason**, like an external bucket that is temporarily unavailable (EODATA or
  even one of our buckets). In that case we exit with a **non 0 status code**,
  so that **Nomad will try** to execute the job again, hoping the external error
  reason disappears (like a bucket that is available again).

The worker software is installed on each worker and is launched by a **simple
`run.sh` script** that ensures the S&I software docker image is available on the
host and launches the Python software described above.

During the pre and post processing steps, the worker needs to **access** to some
**buckets**. For that, we need to use **S3** access and secret **keys**. There
are two kind of buckets that needs come credentials:

* The **internal bucket** (for accessing auxiliary data, the S&I docker
  image...) which credentials are given by two environment variables:
  `CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY` and
  `CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY`.
* The **bucket** for the FSC and RLIE **products** which credentials are given
  by two environment variables: `CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_ACCESS_KEY`
  and `CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_SECRET_KEY`.

The values for all these parameters are set during deployment.

## S&I software

The S&I software runs in a **docker** image. It takes a parameter file and a
shared working directory. There is **no interaction** with the rest of the
**system** beside its input and its output.

An important constraint is that it needs **lot of disk space** to properly work.
Up to 30 GB are needed.

It use a **temporary** directory that will be very big. It is a configuration to
ask for its deletion at the end of the processing, which we are doing because it
is mainly useful for local computation, for example during debugging. In the
operational context this huge directory is not useful and can take too much
resources. We also force its deletion in case of a S&I software crash and to be
sure it won't be upload with the rest of the working directory at the end of
then job.
