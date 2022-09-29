# Operation documentation

The main entry point for high level operation activites is the SIOP document
which is part of the official system documentation.

This documentation is a complement to the SIOP and is focused on internal
operation activities about the the Hr-S&I system, as well as the management
of the infrastructure. It is used to help operators during the operation to
monitor, operate, admin and debug the HR-S&I system.

# Admin VM

## Presentation

An special VM instance can be created and be used for administration tasks.
Its name is `tf-toto` and to create it you should use the deploy script by
specifying the `module.admin` resource (as explained in the [build and
deploy](build_and_deploy.md) documentation).

This instance is configured so that from there you can connect to any other
instance in the system.

It already contains some tools and configuration:

* The **rclone** tool that can be used to manipulate the buckets of the system.
* The **OpenStack** command line tool and the configuration for the project.
* And the Hashicorp tools: **Nomad**, **Terraform** and **Packer**.

## Usages

The admin VM is used to:

* Build the **VM images** (see [build and deploy](build_and_deploy.md)
  documentation).
* Build the **docker images** for the **scientific softwares** (see [build and
  deploy](build_and_deploy.md) documentation). Note that if you have to build
  the si_software base image, it can be time consuming. To do it faster you can
  choose an instance flavor with more VCPUs and change the parallelism option
  in the Docker file of the base image.
* Build the software and instances init **packages** (see [build and
  deploy](build_and_deploy.md) documentation).
* **Deploy** the system (see [build and deploy](build_and_deploy.md)
  documentation).
* **Investigate** on problems from the system internal structure and instances.

# Job execution analysis

## Informations about a job

The main entry point for job analysis is the **HR-S&I dashboard**. It displays
lots of information in different ways:

* The **Jobs summary** tab shows some **geneal statistics** on the previous
day daily production and draw graphs to represent the turnaround met. Few
examples below :
  * The number of job created in the last 24h.
  * The number of finished jobs.
  * The number of generated products.
  * The number of jobs in error.
  * ...

* The **jobs list** tab shows some **detailled** information about the job content.
In it, you can find :
  * The **JSON content** panel, which displays all the job information in a
    **raw format**.
  * The **status changes** panel, in which are listed the status through which
    transitioned the job with their dates and messages.
  * The **logs** panel in which can be found log messages from the `run_worker`
    module and the scientific softwares.

Some **links** are available to be redirected to :

* the **Nomad** last allocation for the job,
* the directory in the `hidden_value` **bucket** where are stored all the
  **logs**, and, in case of error, the S&I processing **working directory**
  (without the L1C product) at the time of the error.

## In case of error

### Dashboard

If an error occurs, the **first step** is to look at the **error subtype** and
**message** in status changes view. Sometimes this is sufficient to understand
what happens.

If it is not enough, the next step it to look at the logs (in the log view):

* first the `run_worker` logs, labelled as `Nomad stdout` and `stderr`,
* then repeat for scientific software processing logs.

**Note**: At the beginning of the `Nomad stdout` log, there is the **name**
and the **IP address** of the **worker** that has run the job. Just be informed
that as workers are continually scale up and down, it is possible that at the
time you look at this log, the worker might have been deleted. Maybe another
worker has been created with the same name and another one with the same IP
address in the mean time.

### Nomad

**Sometime** it can help to go to the **Nomad Dashboard** page of the
**allocation** of the job, but this has to be done relatively **shortly** after
the error occurrence because Nomad cleans allocations information after some
delay (probably several minutes/hours). In this page, among other things, one
can see which worker has run the job, the Nomad events linked to the job, the
main logs (Note that they are the same than in the HR-S&I dashboard log view).

### Working directory of the job

If an error occur inside a scientific software processing, and the logs are not
detailled enough to identify the issue, one can pursue the investigation by
taking a look at the processing working directory. Note that with this
directory one can easily attempt to run the scientific software locally for
further debugging.

To access a scientific processing **working directory**, one can just click on
the `open` link of the related job, in the `Job files` column of the `Job list`
table, in the HR-S&I dashboard. This will automatically redirect the user to
the **OpenStack** dashboard (Note that it requires to log in), in the S3
storage section, and open the path under which is stored the folder.

Note that you can also download the **working directory** on your local machine
if needed using **Rclone** together with the job `unique_id` (which can be
found in the HR-S&I dashboard, in the job `JSON content`).  
For instance the **working directory** of a job with a `unique_id` set to
`7f177eb3-79f1-4ff0-9df3-13d7a75cd526` could be downloaded from the `tf-toto`
instance with the command below :

```shell
rclone copy foohidden_value/work/jobs/7f177eb3-79f1-4ff0-9df3-13d7a75cd526 .
```

* **Warning** : this command downloads the content in the current directory, i.e.
it won't create a `7f177eb3-79f1-4ff0-9df3-13d7a75cd526` sub directory.

If you want to **run locally** the **scientific software** from a downloaded
working directory, you can do it on a machine that has the scientific software,
either installed or in a docker image. You also need to download the related
input product and put it in the adequate directory (for instance : `l1c` for
FSC/RLIE jobs).

# Workers

## Presentation

The core of a job processing is done on a worker, which is a **VM instance**
punctually created to run the scientific software. The number of workers is
**scaled** up and down depending on the number of jobs that are **queued** by
**Nomad**. This scaling is done by the **`worker-pool-management`** service
which asks **OpenStack** to create/delete workers instances depending on the
needs. See the appropriate sections in [system.md](system.md) for details.

When workers are being created, there is a **delay** (few minutes) between the
OpenStack **creation request** and the time it's properly detected by **Nomad**.

## Workers monitoring tools

We have some tools to investigate the state of the workers in our system:

* By looking at the **job list** table in the **HR-S&I dashboard** one can see
  if there are some **running** or **queued** jobs.
* The **worker** page in the **HR-S&I dashboard** shows the **number** of worker
  instances seen by **OpenStack** and the number of worker seen by **Nomad**.
  The list of OpenStack instances not yet seen by Nomad is also displayed in a
  table with their **"age"**, i.e. the duration since it was created by OpenStack.
  There is an **alert icon** (⚠) to identify workers that are quite **old** but
  not seen by Nomad.
* The **`worker-pool-management`** service **logs**, either from the Nomad
  dashboard (to see the service current state) or on the `tf-tata`
  instance for older messages (see the "Nomad logs" section in
  [system.md](system.md)).

## Workers monitoring tasks

Based on these tools, below are some monitoring advices to ensure there are no
issues with the workers and their scaling.

### No jobs in queued/running state

If there is no queued or running jobs, there is **no need for workers**
instances. Then, the number of active worker should be 0. If it is the case
everything is fine.

If the number of workers **is not 0**, maybe it is because the last running jobs
finished recently and the **downscale** process is on going and **not yet
finished**. To check that, you check the current **`worker-pool-management`**
service **log**. If there are some recent messages saying that some workers are
being deleted by OpenStack everything is fine. Wait some time (which can be
quite long if there are lots of workers instances to delete) and when you see
that the `worker-pool-management` service is done deleting instances, the
number of workers must be 0.

If the **number** of workers is **not 0** and the  `worker-pool-management`
service is **not deleting** workers it's probably linked to an issue. This is
considered as a **bug** (most likely in the `worker-pool-management` service)
and some **action** must be taken to identify the problem and fix it. This
should be considered a **major** issue but not a blocking one, as in shuch
situation there is no need to run any processing.

### Queued jobs

If there are jobs in the Nomad **queue** and the number of workers is **lower**
than the **maximum** allowed in the system (210 at the start of the operational
phase for instance), the **`worker-pool-management`** service must being asking
OpenStack to **create** some worker. This can be checked by looking at the
current service **log**.

If it's not the case, this is considered as a **bug** (most likely in the
`worker-pool-management` service) and some **action** must be taken to identify
the issue and fix it. This is considered a **blocking** issue because the system
is not able to create workers as needed and the HR-S&I system is either slowed
down (if few workers are running) or blocked (if no workers are running) until
it's fixed.

### Workers in stale state

Some **errors** during a worker **creation** can prevent the Nomad agent to
start. This put the worker in an intermediate state, as Nomad is not able to
detect it to run scientific softwares processings on it. We call these workers
**"stale" workers**. This appears in the worker page of the dashboard, in the
table of worker that are not registered in Nomad and that are "old" (created
more than 15 minutes ago, with the alert icon ⚠). It's the job of the
**`worker-pool-management`** service to **detect** such stale workers and to
**delete** them. It can be monitored in the service **log**. But be aware that
the service don't delete any instance while there is a need for new workers to
be createed (which is seen as a priority).

**If** the service **doesn't delete stale** workers when ne new instances are
needed, this is considered as a **bug** (most likely in the
`worker-pool-management` service) and some **action** must be taken to
investigate on the issue and fix it. This is a **major** issue, but not a
blocking one, because while inconvenient this doesn't prevent processing to run
on normal workers. And it is safe to delete manually the stale worker (by using
OpenStack command line tool for example).

## Managing workers

If one needs to manage workers manually he can use the OpenStack Python SDK
(see how it is used in the `worker-pool-management` Python code) or the
OpenStack command line tool.

### Delete a worker

To **delete** a worker that **is not running a job** (like a stale worker),
simply use the command:

```shell
openstack server delete os-worker-042
```

You can specify several worker names in this command.

If the worker is **currently running** a S&I processing **job**, deleting the
worker will put the job in an "intermediate" state, i.e. the processing will
stop while the status in the database will be one of the running status. A
priori, **Nomad** will detect this and **reallocate** the job to another worker
that will restart the processing from the beginning. The impact is minor :
there is a loss in processing time, and thus, in processing cost.

If you want to **delete** a worker that is currently running a job processing
but want to do it once the processing is **complete**, you can switch off its
**eligibility** in the **Nomad** dashboard. This way, you will be sure that
Nomad won't allocate a new job after the current one. **Note**: you can also
switch off the eligibility with Python code (see `worker-pool-management`
Python code for an example).

### Create a worker

The creation of a worker uses the **image** of the disk of the
**`tf-titi`** instance and sets the appropriate security groups and
networks. While this may possible with the OpenStack **command line** tool, it  
might be **tricky**. The **best** way would probably be to write a **Python
script** with apropriate code copied from the `worker-pool-management` Python
module.

**Warning**: doing so, the **Nomad agent** will be **started** at the start of
the worker, which will make it **available** for **running** a job and Nomad
will allocate one very **quickly** if there are some jobs in the queue. If you
need a worker but don't want it to join the Nomad pool, you will have to find
another way to do that (maybe creating an instance and installing the worker
software manually).

# Nomad

There is not much to say about Nomad for operation. The only thing to monitor is
the Nomad server resources use (CPU, RAM and disk) but a priori the system
doesn't load to much the server.

## Managing nomad services

These services are automatically launched on a new deployment and appears in the
Nomad dashboard, acessible from the `Nomad dashboard` tab of the HR-S&I
dashboard. They can be managed in two different ways :  

### Managing Nomad services with HMI

The Nomad dashboard, accessible from the HR-S&I dashboard, can be used to easily
start / stop services using an HMI. Indeed, you just need to click on a service
instance from the dashboard and then press the `run` / `stop` red button on the
upper right corner of the HMI.  
Note that you can `run/stop` a Nomad service from either the Nomad service
main page, in which case a new **Nomad allocation** will be created, and the
logs from the previous one will be lost. Or you can just `restart` the running
Nomad allocation from the `job-NAME/job-NAME/ALLOCATION-ID/job-NAME` Nomad
dashboard page. In this second case, there will be no discontinuity between the
previous allocation run logs, and the current one.

Note that a stopped service will disappear from the Nomad dashboard
automatically after some minutes.

### Managing Nomad services with command line

To do so, you should be connected to the `tf-tete` machine (this can be
done by sourcing an appropriate OpenStack bash file, using the
`cosims/deployment/ssh_connect_to_admin.sh` script to connect to the admin
machine, and from there run an `ssh` command to connect to the `tf-tete`
machine). Once connected to the `tf-tete` machine the different services
Nomad files can be found under
`/opt/csi/init_instance/nomad_server_instance_init_git-hash-{YOUR_DEPLOYMENT_HASH}/`.
You can move to this folder, and run the commands `nomad run` / `nomad stop`
followed by the Nomad file of the service you want to start / stop. For
instance, to stop the publication service you should run
`nomad stop job_publication.nomad`.

Note that this command line can be used to restart a Nomad job which
disappeared from the Nomad dashboard.

# Orchestrator services

The orchestrator is composed of 6 main services interacting with the database
to create/process jobs and send the generated products to the endpoint APIs.  
These services logs and health state can be retrieved in the Nomad dashboard.

## Job creation

This service is in charge of creating new entries in the **database**, under the
form of **jobs**. To do so, it requests the DIAS API, and the HR-S&I API for new
input products every 5 seconds, if they don't already exist in the **database**,
new **jobs** are created.

## Job configuration

The **job configuration** service is in charge of retrieving, computing essential
information for the job processing, and store them in the job JSON data.  
It also organizes the jobs execution order to respect time-series.

## Job execution

This service affects a Nomad allocation to a given job, which went through the
**creation** and the **configuration** procedures, to process it on a dynamically
created VM.

## Job publication

The **job publication** service fetch the jobs which have been processed, it then
build and send a JSON containing essential information on the job through a
RabbitMQ queue.  
Upon recieving the JSONs, the RabbitMQ server is then able to index the products
that we processed, and stored on an S3 bucket, to make them available to endpoint
users.

## Worker pool management

This service is in charge of creating and destroying workers, used to process jobs,
when needed.

## Monitor

The **monitor** service ensure that none of the jobs being executed are encountering
trouble with Nomad. If they do it will automatically put them back in the queue to
get a new Nomad allocation and be processed again.
