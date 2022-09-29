# Stop/Start the operational system

>This document provides hints and good practice rules on the **operational system**
management.  
These recommendations can also be applied to the **test system**
management, which is a copy of the **operational system**, totally independent
from it, hosted on a different **OpenStack** project (`csi_test`), with its own
independent buckets. The **test system** management differs slightly from what is
described below as the scripts used to start/stop the system are prefixed by `test_`
and require different access keys.

Stopping, starting the operational system are operations that can have significant
consequences on the operational system integrity, and thus should be done carefully,
with a clear plan of the actions to conduct in mind.

These operations are performed from the `tf-toto` instance, from which you can
connect to any system-related VM.

## Connect to the tf-toto instance

More details on the `tf-toto` instance can be found in [operation.md](operation.md).

To connect to this VM you should have some **OpenStack** environment variables set.
The proper way to set this environment is to **source** the `OpenStack RC v3` file
which can be downloaded from the **OpenStack** dashboard once you are logged in
and you selected the appropriate project.
To download the mentioned file, you should select your **username** on the
upper right corner of the dashboard window, and select the `OpenStack RC v3` line.

Once the **OpenStack** environment variables are set you can simply run the
[ss_connect_to_admin.sh](../deployment/ssh_connect_to_admin.sh) script to connect
either to the **production** or **test** `tf-toto` instance, depending on the
envrionment you sourced.

## Stop

To stop the operational system, you can simply call the [prod_env_destroy.sh](
../deployment/prod_env_destroy.sh), with the following argument `module.core`.

* ex : `prod_env_destroy.sh module.core`

Note that the script mentioned above require some **access/secret keys** to run.
These keys should be stored in two different files :

* [prod_env_configuration.tfvars](../deployment/prod_env_configuration.tfvars) :
for the **access keys** (the file can be found in the git repository).
* `prod_env_secrets.tfvars` : for the **secret keys** (the file is not stored in
the git repository as it contains critical informations, but an example of its
content can be found there :
[test_env_secrets.tfvars](../deployment/files_examples/test_env_secrets.tfvars)).

After running the command above, **Terraform** will perform a check-up of the
actions that will be performed, and ask for your approval. at the time this
document was written, a classic **operational system** destroy operation should
destroy **13 elements** :

* `Plan: 0 to add, 0 to change, 13 to destroy.`

Once you gave your approval, all the system instances will be destroyed :

* `tf-tutu`
* `tf-tete`
* `tf-tata`
* `tf-titi`

Note that a backup of the database content is saved, and automatically re-used
on the next system deployment.

## Start

To start the operational system, you can simply call the [prod_env_apply.sh](
../deployment/prod_env_apply.sh), with the following arguments `module.core`.

* ex : `prod_env_apply.sh module.core`

Note that the script mentioned above require some **access/secret keys** to run.
These keys should be stored in two different files :

* [prod_env_configuration.tfvars](../deployment/prod_env_configuration.tfvars) :
for the **access keys** (the file can be found in the git repository).
* `prod_env_secrets.tfvars` : for the **secret keys** (the file is not stored in
the git repository as it contains critical informations, but an example of its
content can be found there :
[test_env_secrets.tfvars](../deployment/files_examples/test_env_secrets.tfvars)).

After running the command above, **Terraform** will perform a check-up of the
actions that will be performed, and ask for your approval. at the time this
document was written, a classic **operational system** start operation should
create **13 elements** :

* `Plan: 13 to add, 0 to change, 0 to destroy.`

Once you gave your approval, all the following system instances will be created :

* `tf-tutu`
* `tf-tete`
* `tf-tata`
* `tf-titi`

Note that a backup of the database content is saved, and automatically re-used
on the system deployment.

### Starting the system right after a stop

If the delay between the system stop and its restart remains short :

* few minutes to 1 day maximum,

the procedure described in the previous `Start` section can be applied
with no limitation.

### Starting the system after a long downtime period

If the delay between the system stop and its restart is long :

* more than 1 day,

the procedure described in the previous `Start` section is still applicable,
however the full system restart should be performed progressively.

Indeed, at the time this document was written, the operational system was handling
the production in NRT of 7 different jobs, with some that depends on others.
Therefore, the jobs that are located at the end of the dependency chain should
only be restarted once all the others have caught back the NRT production.

The different jproducts could be classified as follow :

* no internal dependencies : `FSC`, `RLIE(S2)`, `RLIE-S1`,
* light internal dependencies : `SWS`, `WDS`, `RLIE-S1-S2`,
* heavy internal dependencies : `GFSC`,

As `FSC` and `RLIE(S2)` are grouped behind the same job type : `FSC/RLIE`, just
like `SWS` and `WDS` with `SWS/WDS` job type, we could orchestrate a long downtime
system restart as follow :

* 1st wave : `FSC/RLIE`, `RLIE-S1`,
* 2nd wave : `FSC/RLIE`, `RLIE-S1`, `SWS/WDS`, `RLIE-S1-S2`,
* 3rd wave : `FSC/RLIE`, `RLIE-S1`, `SWS/WDS`, `RLIE-S1-S2`, `GFSC`

with each wave being started once the job types present in the previous one caught
back with the NRT production completely. Meaning that they are being generated,  
published, and indexed in the FINDER/WMS APIs in NRT.

To specify which job types the system should handle, you should list them in the
`job_types_list` system parameter (cf :
[system_parameters.py](../components/common/python/database/model/job/system_parameters.py)).
For instance to handle all the job types, the following value should be set :

* `"['fsc_rlie_job', 'rlies1_job', 'rlies1s2_job', 'sws_wds_job', 'gfsc_job']"`.

Note that most of the job types automatically detect the last job inserted in the
database of their given type, so that they will request APIs for new input product
starting from the last one input product's publication date.  
At the time this document was written, the only job type not following this workflow
is `RLIE-S1-S2`, which automatically perform a backward search of several days
(minimal number of days set in system parameters `rlies1s2_min_search_window_days`
cf : [system_parameters.py](../components/common/python/database/model/job/system_parameters.py)).
Therefore, keep in mind that the `rlies1s2_min_search_window_days` and thus the
`rlies1s2_max_search_window_days` values (backward search maximal number of days)
should be greater than the **operational system down time**.

Note also that GFSC jobs have a special workflow for daily jobs, which consists
in automatically producing a job for tiles on which there weren't new input data
on a given date. This process is triggered at night, and at any system restart
automatically covers the two past days. If the **operational system down time**
was greater than two days, you will need to set the date from which you want the
system to start checking for new daily GFSC jobs in the `gfsc_daily_jobs_creation_start_date`
system parameter (cf :
[system_parameters.py](../components/common/python/database/model/job/system_parameters.py)).

# Patch the system without restart

It's important to have in mind that some parts of the system can be updated without
restarting it. In the next sections are detailed these components, the way they
can be modified, and how they impact the system.

## Patch the database definition

If you need to update the definition of a table, a function, or any other element 
stated in the database SQL file [init_database.sql](../components/database/init_database.sql), 
you should create an **SQL patch file**. The previous patchs that have been applied 
are stored under `components/database/patch/`, they can be used as examples.  
Once the database patch script is ready, you should first **test it on a backup of 
the database** to ensure that it won't break anything on the production one. To 
do so, you can connect to the `tf-tutu` instance, and create a save of the 
current database content by running the following command : 
- `sudo -u postgres pg_dumpall -s > /tmp/YYYY.MM.DD_dump.sql`

Then you can copy the database dump on the `tf-toto` instance, to then fetch it 
on your local machine, in your own `/tmp/` folder using `scp`. Once this is done, 
you should remove the previous database content loaded by running the line below. 
**Warning : Note that this is a highly critical command which should never be 
run on the production environment `tf-tutu` instance, except if you are certain 
that you know what you are doing !** Indeed, this command would remove the entire 
content of the running database, so please use it carefully, and make sure that you 
are located on your local machine before launching it. First open the SQL command 
terminal : 
- `sudo -u postgres psql`

Then, in it run the **highly critical command** (cf. few lines above) : 
- `DROP schema cosims cascade;`

Afterwards you might load the database dump you created some minutes ago. First, 
move to your local `/tmp/` folder, then load the database save, with the following 
command : 
- `sudo -u postgres psql -f /tmp/YYYY.MM.DD_dump.sql`

Copy your migration script in your local `/tmp/` folder, and then, apply it to the 
loaded database with the line below : 
- `sudo -u postgres psql -f PATCH_FILENAME.sql`

Ensure that **no error were raised**, and if it's the case you can proceed to patch 
the production database. You just have to copy your script on the `tf-toto` instance, 
and from there, to copy it on the `tf-tutu` one, in the `/tmp/` folder. Place 
yourself in this `/tmp/` directory, and run the following command : 
- `sudo -u postgres psql -f PATCH_FILENAME.sql` 

The database defintion must have been updated !

## Patch system parameters table content

The system parameters table, is a direct interface with the operational system.
If an element is updated in it, it would automatically be taken in account by the
system. Please find below, as an example, a non-exhaustive list of data that can
be found in this table :

* the number of workers instances allowed
* the types of jobs handled
* the backward search for S1/S2 based jobs (in case the database is empty)
* GFSC gapfilling default duration
* MAJA configuration parameters
* RabbitMq publication endpoint url
* docker images IDs for scientific softwares
* SWS/WDS auxiliary data version
* services sleep time between two executions
* RLIE-S1-S2 window search parameters
* GFSC daily job creation starting date

One easy way to update the system parameters table content, is to connect to the
**database VM** and to run an **SQL command**.  
To do so, from the `tf-toto` instance you can connect to the `tf-tutu` one
with a simple `ssh` command :

* `ssh eouser@<TF_DATABASE_PRIVATE_MAGELLIUM_IP>`

And once you are on the `tf-tutu` VM, you can open the SQL command terminal
with :

* `sudo -u postgres psql`

Find below an example of **SQL code** to update the `max_number_of_worker_instances`
param value, from the system parameters table :

```sql
update
    cosims.system_parameters
set
    max_number_of_worker_instances = 3;
```

## Patch the system database jobs

A **script** exists to **update** one or several **jobs** of a given type in the
database : [patch_database_jobs.py](../common/patch_database_jobs.py). It can be
used to **update** the jobs **status** (most of the cases), when these ones are
stuck in a given status and need to be put back in an earliest one (many jobs
stuck in error for instance). You can also use it to fix the **JSON content** of
a job or a list of jobs, but it's less likely to happen, and keep in mind that
this action is more **critical** as you might interfere with the system nominal
workflow.

This script requires several **environment variables** to be defined to run :

* `COSIMS_DB_HTTP_API_BASE_URL` which should be set as `"http://<tf-tutu-instance-IP>:3000"`,
* `CSI_SCIHUB_ACCOUNT_PASSWORD` which should be set to the Scihub account password
(can be found in the `prod_env_secrets.tfvars` file on the `tf-toto` instance),
* `CSI_SIP_DATA_BUCKET` which can be set to a random value.

To prevent relative import related issues to occur, this script should be run as
follow :

* `python3  -c "from RELATIVE.PATH.TO.patch_database_jobs import update_jobs_status; update_jobs_status()"`

Also, do not forget to update the relative imports defined in the script if you
whish to place it in a different folder than where it was orginially designed to
be located in : `components/database/`.

Three methods are defined in it :

* `update_jobs_status()` : designed essentially to update a **job** or a **list
of jobs status**, in case they are **stuck**. Please refer to the `cosims.check_status_change()`
method defined in [init_database.sql](../components/database/init_database.sql)
to check the **authorized status transitions**. This method can also be used to
update a **job JSON content**, for that refer to the lines commented right after
the following comment : `Update the job JSON content if needed.` in the script.
First, affect the value to the job parmeter(s) you want to update (example :
`job.l2a_status = L2aStatus.pending`), then, uncomment the call to the **patch()**
method (example : `job.patch(patch_foreign=True, logger_func=temp_logger.debug)`) .
As stated above, keep in mind that this last point is **critical**, so only proceed
if you are confident with your update.

* `patch_jobs_in_error_with_invalid_L1C()` : this method **shouldn't be needed
anymore**. It was created at a stage of the project where some FSC/RLIE jobs were
getting **stuck** and **couldn't generate** ouput products because of **corrupted
L1Cs**. It was designed to **select** jobs based on their **L1C IDs**, and update
their **status** and their **JSON content** to notify the system that they wouldn't
generate products, and thus wouldn't need to be published.

* `fix_backward_time_series()` : this method **shouldn't be used as it is**. It
was a work in progress, that was designed to fix FSC/RLIE time series on a given
tile ID, when issue were noticed with **backward** jobs triggerred to replace
**init** and the following **degradded quality** jobs. If such problem were to
occur again in the system (most likely around February/March, once the polar
night is over), you could get **hints** on how to proceed from the logic in this
method.

To use the `update_jobs_status()` method, in the first section, uncomment the
**type of jobs** you whish to select, as well as the **status list** based on
which you want to filter the jobs. You should end up with something similar to
the line below (not taking in account the comments) :

* `jobs = StoredProcedure.jobs_with_last_status(FscRlieJob,[JobStatus.internal_error],logger_func=temp_logger.debug)`

Note that if you whish to select jobs in  a **status** where **numerous jobs**
are (**done** status for instance), you should rather use the **second jobs selection
method**, as the first one described above will **fail**. The second method use
a **time range filtering**, which implies to define a lower, and an upper time
bound. You should also mention the name of the parameter (of type timestamp) in
the jobs JSON, on which you whish to apply the time filtering. This second selection
method could be used as follow :

```python
search_start = datetime(2022, 1, 23)
search_end = datetime.utcnow()
jobs = StoredProcedure.get_jobs_within_measurement_date(FscRlieJob(), 'measurement_date', search_start, search_end, temp_logger.info)
```

Then, you can apply some additional filtering, based on the jobs JSON content,
such as their ID for instance :

* `jobs = [job for job in jobs if job.id in [366230, 366231]]`

Then you should specify which status will be applied to the selected jobs. For
instance, to set successively the `error_checked` and the `ready` status, the
`for` loop should looks like that (not taking in account the comments) :

```python
for job in jobs:
    time.sleep(1)

    '''Update the job JSON content if needed.'''
    print('Updating job %s with current status %s...' %(job.id, JobStatus(job.last_status_id).name))
    job.post_new_status_change(JobStatus.error_checked)
    print('    -> new status error_checked')
    job.post_new_status_change(JobStatus.ready)
    print('    -> new status ready')
```

## Patch scientific software images

The scientific software image ID used for every type of job by the HR-S&I system
is stored in the **system parameters** table. Once a new image is available on the
bucket, you can simply follow the instruction detailed in the previous section for
the system to use it.

To generate a new docker image for a given software you can simply run the appropriate
script located in the HR-S&I git repository under : `build/docker/images/`. The
folder tree architecture is organized as follow :

* `gfsc_software/build_gfsc_image.sh` : for GFSC products.
* `si_software/build_base_image.sh` : to generate the base image used to build
the FSC/RLIE products final image (Note that the base image does not change often).
* `si_software/build_final_image.sh` : for FSC/RLIE products.
* `si_software/build_part2_image.sh` : for RLIE-S1/RLIE-S1-S2 products.
* `sws_wds_software/build_sws_wds_image.sh` : for SWS/WDS products.

Note that to run these scripts you need to have **Rclone** configured on your
instance with an endpoint called `[csi]` pointing to the OpenStack project you
want to upload the scientific softwares images to.  
The softwares docker images are uploaded to the `foo` bucket, under
`docker/images/`, with a name such as `image_name` for FSC/RLIE
products. `git-b7e71dc4` being the latest **git commit hash** when you built the
image.  
Note also that you should have enough **disk space** available on your instance
to be able to build the docker image (about 2 to 8GB depending on the image).

## Patch single orchestrator service

Orchestrator services are running in docker images. These docker images are stored,
and fetched from the GitLab repository's **Container Registery**. It can be accessed
from the **Packages & Registeries** tab ([GitLab HR-S&I
project](https://gitana-ext.magellium.com/cosims/cosims/container_registry)).  
There is an image for each service :

* **job creation**
* **job configuration**
* **job execution**
* **job publication**
* **worker pool management**
* **monitor**

Once a new image is available for a given service, it can be easily put on the
production environment by editing the associated service **Definition** in the
**Nomad dashboard**. The image tag in the following line should be updated :

* `"image": "path_to_image",`

Note that the new image tag value manually set in the **Nomad dashboard** is not
saved at system level. Therefore, if you restart the entire system, the value manually
set would be overloaded by the one saved at system level, stored in
[job_creation.nomad](../build/instance_init_packages/nomad_server/src/envsubst/job_creation.nomad)
for the creation service for instance. If you want your patch to be persistent,
do not forget to also update the image tag in the associated service **Nomad** file,
the file mentioned in the previous sentence.

To generate new images for the orchestrator, refer to
[build_and_deploy.md](build_and_deploy.md).

## Patch HRSI dashboard

Once you have committed your changes on Gitana, and tested them locally (you can 
refer to the dashboard readme file [README.md](../components/dashboard/README.md) 
for local deployment), you can build the dashboard packages witth the following 
command : 
- `yarn build` (in `cosims/components/dashboard`)

This will generate a `dist/` folder that can be **tared** and copied through **ssh** 
on the Magellium dashboard machine under `/var/www/temp/`.  
There, you can **untar** this folder, rename it to `html/`, and move it to the 
parent folder `/var/www/`.  
Note that there should already be an `html/` folder there that you might want to 
keep a copy of, to have a **backup of the latest stable version** of the dashboard.

Once you replaced the `html/` folder you can **reload** the dashboard, your update 
should be available.

# Reset the database content from a backup

To get more details on the database backup mechanisme, please refer to the
[backup.md](backup.md) documentation.

Before performing a database reset, there are few points that you should have
in mind :

* Once completed, this operation **can't be reverted**,
* There must be a system, or at least a database running,
* You might want to stop some of the orchestrator services before doing it, to
better control the operational system restart.

Keep in mind that services stopped for couples of minutes might disappear from
the Nomad dashboard. To restart them you would need to connect to the
`tf-tete` instance (available in ssh from `tf-toto` one), move to the
`/opt/csi/init_instance/nomad_server_instance_init_git-hash-<commit-hash>` folder
and run the command below. They should automatically be back on the Nomad dashboard
after that.

* `nomad job run <nomad-job-file>.nomad`

The available database backups can be listed and extracted thanks to the **borg**
tool (detailed in the [backup.md](backup.md) document). Once you selected which
backup you want to load and extracted it as a `database-all.sql` file, you will
need to **remove the running database content**. Indeed, a backup can only be
loaded to an empty database. Note that this is a **highly critical** operation
that can't be reverted, so it should be performed carefully. To reset the database
content you should first open the PostgreSQL interactive terminal with the following
command :

* `sudo -u postgres psql`

And then run the follwing line in it :

* `DROP schema cosims cascade;`

Then, to load the backup you can use the line below in the linux terminal :

* `sudo -u postgres psql -f database-all.sql`

Note that depending on the delay between the date the backup was made and the day
you are performing the database reset, you will need to adapt the procedure to
restart the overall operational system. Indeed, if the delay is lower or equal to
**1 day**, you should follow the guidelines detailled in the **"Starting the system
right after a stop"** section of this document. Otherwise, please follow the
instructions described in the **"Starting the system after a long downtime period"**
section.

# Workflow of calssic new patchs deployment

Once an issue has been identified, traced in Gitana, that a patch has been
developped on a new branch, linked with a merge request, tests have been performed,
and you consider that you are ready to apply the patch on the production environment,
you might want to follow the guidelines below.

## Merge your development branch

Make sure to **solve your merge request** linked to the gitana issue you opened
for your patch to be passed in the most up to date branch, used on the deployment
environment.

## Generate docker images for the orchestrator services

The docker images are **built** by a manual **GitLab CI** task. To launch the build
from a CI/CD pipeline you should simply **click** on the "push services" in the
"Deploy" step:

![Screenshot of build orchestrator images from GitLab
CI](images/docker_build_in_gitlab_ci.png)

Once this task is completed, docker images are stored in the **Gitana project
container registry**, tagged with the **git hash** of the commit associated to
the deployed pipeline.

## Update orchestrator services images hashes in Nomad files

The **tag** (commit git hash) under which the services docker images have been
generated should then be replaced in the **orchestrator services Nomad files**
listed below :

* [job_creation.nomad](../build/instance_init_packages/nomad_server/src/envsubst/job_creation.nomad)
* [job_configuration.nomad](../build/instance_init_packages/nomad_server/src/envsubst/job_configuration.nomad)
* [job_execution.nomad](../build/instance_init_packages/nomad_server/src/envsubst/job_execution.nomad)
* [job_publication.nomad](../build/instance_init_packages/nomad_server/src/envsubst/job_publication.nomad)
* [monitor.nomad](../build/instance_init_packages/nomad_server/src/envsubst/monitor.nomad)
* [worker_pool_management.nomad](../build/instance_init_packages/nomad_server/src/envsubst/worker_pool_management.nomad)

Only one line should be updated, the following one :

```nomad
      image = "registry-ext.magellium.com:443/cosims/cosims/:git-<commit-hash>"
```

Then, do not forget to **commit, and push** your changes.

## Stop the production envrionment

Connect to the `tf-toto` instance, move to your project repository `deployment`
folder and stop the production environment. You may refer to the **Stop** section
of this document if you are not sure how to proceed.

## Fetch your changes

Run a `git pull` command on your project repository stored on the `tf-toto`
instance to fetch your latest changes.

## Build instance init packages

Move to the `build` folder of your project directory, and ensure that there are no
local updates that haven't been committed by running the `git status` command. If
there are, proceed to commit them, and got through the previous steps a second time.
If there a none, run `./build.sh` script to generate the **instance init packages**
which will be used to initialize the system instances.

## Restart the production envrionment

You should already be connected to the `tf-toto` instance, so move back to your
project repository `deployment` folder, and as for a patch deployment the operational
system down time should be shorter than 1 day, follow the instruction detailled
in the **Starting the system right after a stop** section of this document.

## Ensure that all the services are properly started

On some occasions, it can happen that a service does not find enough ressources
in the datacenter to be started, most of the time it's the `worker-pool-management`,
which remains stuck in `pending` state in the **Nomad dashboard**. If this situation
occurs, in the **Nomad dashboard** click on the affected service, move to the
`definition` tab, click on the **edit** button, on the upper right corner, and
lower the value set on the following line :

* ~~`"CPU": 1500,`~~ to `"CPU": 1000,`

Then, successively click on the **plan** and **run** buttons, on the bottom left
corner of the page, to apply the changes. The service should be able to start
properly afterwards.
