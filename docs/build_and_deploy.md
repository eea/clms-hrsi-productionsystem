# Build

There are several elements to build in the system. All this take place in the
[build](../build) directory.

## VM images

The directory [build/vm_images](../build/vm_images) contains what is needed to
build the VM images used by the system.  

For now there is **only one** VM image which is used as template. Its building uses the tool **Packer** and must be runned on an existing VM **in the OpenStack project** (like the *tf-toto* instance). This **base** image contains some basic dependencies (like docker).  

The image name contains a **suffix** to identify the **hash** of the **git**
commit that is to be used as source (`git-7907d8e2` for example).

After building, the image is available on the image list of the OpenStack
project.  

The script also configures **rclone** to allow all the instances using this
image to have **access** to the internal **buckets** of the OpenStack project. In
particular, this means that a base image is **specific** to a **deployment
environment**. This is not ideal because two images with the same name are
configured differently. Thus, if one uses the production image in a
test context by accident without knowing this configuration policy, they will modify the production bucket by mistake.

## Docker images for scientific softwares

The scripts to build the Docker images embedding scientific softwares that used to generate Snow & Ice products (**FSC/RLIE**, **RLIE-S1**, **RLIE-S1+S2**, **SWS/WDS**, **GFSC**) are stored in their respective directories under [build/docker_images](../build/docker_images).

### S&I software

Two docker images can be built from the `si_software` directory, the S&I software 
part-1 (generating **FSC/RLIE** products) and the S&I software part-2 (generating 
**RLIE-S1**, **RLIE-S1+S2** products).  
Both S&I docker images are **quite large** (several GB) and are stored in the
**`foo`** bucket. They preferably **must** be built from the **admin VM** 
for performance reason. These images are used by the **workers** which 
**download** them from the bucket when needed.  
This directory contains a [readme.md](../build/docker_images/si_software/readme.md) 
file with precise instructions to launch the docker images build. 

Regarding the S&I software part-1, the main thing to know is that it's built 
from **two images**:

* The **base** image, that mainly contains the external dependencies (OTB, SNAP,
  Maja, etc.). This image takes a long time to build (in the order of tens
  of minutes) but doesn't change very often.
* The **final** image, that starts with the base image and adds our tools to
  execute S&I processings. This build is a lot faster (in the order of
  minutes) and its content changes more often than the base image's one.

The S&I software part-2, on the other hand is only built from a
**single image**, which is pretty similar to the **part-1 final** image in its
characteristics (build time, and update frequency).

### SWS/WDS software

Similarly the SWS/WDS software docker image can be built from the `sws_wds_software` 
directory, using the bash script [build_sws_wds_image.sh](../build/docker_images/sws_wds_software/build_sws_wds_image.sh) 
located in it.  
This directory contains a [readme.md](../build/docker_images/sws_wds_software/readme.md) 
file with precise instructions to launch the docker images build.  
The generated docker image is also **quite large** (several GB) and stored in the 
**`foo`** bucket. They preferably **must** be built from the **admin VM** 
for performance reason.

### GFSC software

Similarly the GFSC software docker image can be built from the `gfsc_software` 
directory, using the bash script [build_gfsc_image.sh](../build/docker_images/gfsc_software/build_gfsc_image.sh) 
located in it.  
This directory contains a [readme.md](../build/docker_images/gfsc_software/readme.md) 
file with precise instructions to launch the docker images build.  
The generated docker image is also **quite large** (several GB) and stored in the 
**`foo`** bucket. They preferably **must** be built from the **admin VM** 
for performance reason.

## Docker images for the orchestrator services

The Dockerfile for each orchestrator service is stored in the source directory
for the service, like
[components/job_creation/docker/Dockerfile](../components/job_creation/docker/Dockerfile)
for the job creation service.

The orchestrator images are stored in the **docker registry** of the [GitLab
CoSIMS project](https://gitana-ext.magellium.com/cosims/cosims). They are
retrieve by the **Nomad** jobs for the **orchestrator** services when they are
started.

The docker images are **build** by a manual **GitLab CI** task. When needed, one
launches the build from the CI/CD by clicking on the "push services" in the
"Deploy" step:

![Screenshot of build orchestrator images from GitLab
CI](images/docker_build_in_gitlab_ci.png)

The build is specified using **docker compose** and uses some environment variables (`CSI_DOCKER_REGISTRY_IMAGE` and `CSI_DOCKER_TAG`) that are automatically set by GitLab CI. This step uses the **git hash** as the **tag** for the docker images like `git-0ade3614`.

One can also build them from the command line. For example, to only build then push the `job_creation` docker image with the `<some-tag>` tag, one can use the following commands:

```shell
sudo CSI_DOCKER_TAG=<some-tag> CSI_DOCKER_REGISTRY_IMAGE="registry-ext.magellium.$ com:443/cosims/cosims" docker-compose build job_creation
sudo CSI_DOCKER_TAG=<some-tag> CSI_DOCKER_REGISTRY_IMAGE="registry-ext.magellium.com:443/cosims/cosims" docker-compose push job_creation
```

## Update the Nomad jobs with new orchestrator docker images

When some **new** images are built with a new tag and to take them into account
in the system, usually the corresponding **Nomad job files** are **updated**. Those files are in the directory [build/instance_init_packages/nomad_server/src/envsubst/](../build/instance_init_packages/nomad_server/src/envsubst/).

One can also update the Nomad jobs configuration on a **running system**:

* Either by **editing** them directly in the **Nomad dashboard**. It is not recommended in production because this is not traced and any job resubmission while overwrite it.
* Or by **editing** the Nomad **files** in the **nomad server** which has been used to create the Nomad jobs to begin with. This is more persistent yet it is not perfect (and not recommended in production) because it is not tracked elsewhere than on the Nomad server.

Keep in mind that Nomad jobs which have been stopped for couples of minutes might disappear from the Nomad dashboard. To restart them you would need to connect yourself to the `tf-tete` instance (available in ssh from `tf-toto` one), move to the `/opt/csi/init_instance/nomad_server_instance_init_git-hash-<commit-hash>` folder and run the command below. They should automatically be back on the Nomad dashboard after that.

```shell
nomad job run <nomad-job-file>.nomad
```

## Software and instance init packages

There are some parts of the system that are packaged in **`tar.gz`** files. This
is the case for some **software components**:

* database
* dashboard
* worker

Each of these components defines a `package.sh` script that creates the package
file.

And this is the case of the set of files needed to **initialize** all the
**instances**:

* database
* nomad-server
* orchestrator
* worker-template
* dashboard (if it needs to be deployed on the OpenStack infra)

Once built, all the packages are **stored** in the **`foo`** bucket and retrieved from there when needed.

There is a high level script that **builds all** these packages using the current **git commit** hash as a **tag** (like `git-hash-7907d8e2`):

```bash
cd build
./build.sh
```

If the script detects **any changes** since the last commit, it actually uses the tag `git-hash-7907d8e2-changed` to **explicitly** state that the package contains untracked changes.

The build and push of the packages are **defined** in the following directories:

* [build/components_packages](../build/components_packages)
* [build/instance_init_packages](../build/instance_init_packages)

# Deployment of the infrastructure

## General information

The infrastructure is deployed using **Terraform**. See the directory [deployment](../deployment) and its ReadMe for more information.

Yet here are the main elements to know:

* There are **scripts** to **apply** Terraform changes and to **destroy** resources.
* There are **two** deployment **environments**, one for **production** and one for **testing**.
* There are **separate** scripts for **each environment**, which **check** that the target OpenStack **credentials** and **project number** are the correct ones. This is useful to protect the user from deploying in production by error while he thought he was targeting the test environment.
* There are some **configuration files** to create/update before deploying (see the ReadMe in deployment directory) where in particular the secrets (like credentials bucket access, etc.) are set.

## Database

It is also important to know that the complete system **can** be **deployed again**
from scratch while **keeping** the **database**. This way, we can keep an
existing and valid database and deploy a fresh up to date system. Of course if the
database needs some changes to fit with the new system, some specific actions
must be taken (like applying a SQL patch script).  

If one **needs** to deploy a new system with a **fresh database**, then the Terraform **resource** for the database **disk** must be **destroyed** before
(at the time of this writing this resource is
`module.cold.openstack_blockstorage_volume_v2.database`). As this is a
**sensitive** resource, it is **protected** in its Terraform file with the
`prevent_destroy` set to `true`. If you really need to destroy it, you first
have to set it to `false`, launch the destroy script on it, then switch back the
property to `true`. **Important**: don't forget this step and more importantly
don't commit this property with the `false` value, as the resource won't be
protected anymore.

## Terraform modules: core, cold, admin and buckets

The **resources** managed by Terraform are **separated** into several Terraform
modules depending on their typical **life cycle**.

* The module `buckets` contains the definition of the **buckets** that are central to the internal behavior of the system (like `foo` or `hidden_value`). See [buckets.md](buckets.md) for more information about the buckets. These resources are protected (see below) so that they can't be deleted by error.
* The module `cold` contains the resources that **don't change often** or that need to be **frozen**. These resources are protected (see below) so that they can't be deleted by error. For example this module contains:
  * The **database block storage** resource that often needs to be kept between two deployments.
  * The **external IP** of the HTTP proxy for the CSI services.
  * Some **ssh key** pairs.
* The module `core` contains the **main** resources of the system that **change "often"**, like the instances for the database, the Nomad server, the orchestrator...
* The module `admin` contains all that is needed to create the admin instance.

A **protected** resource, is a resource that contains the following attribute:

```bash
  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
```

With this, if one tries to destroy a protected resource, Terraform will exit with error. If you really need to destroy such a resource (like the database block volume for a full reset of the system), first comment this few lines, destroy the resource
and, **most importantly**, uncomment them back.  

# What happens during a deployment?

To help understand what is going on, and why the things are defined the way they
are, this section details the **flow of actions** when one launches a **deployment from scratch**.

So, consider the system is not deployed on the production environment. Your OpenStack configuration in your shell points to the `os-automation` account and
the production project. You execute the following commands:

```shell
$ cd build
$ ./build.sh
...
$ cd ../deployment
$ ./prod_env_apply.sh module.core
```

Here is what happens:

1. The script `build.sh` gets the current **git commit hash** and uses it to    define the **package tag** to use. If there are no differences with the current state of the branch you are on, the tag will be something like `git-hash-220f73f3`.

2. As explained before, this script will **build all packages** (software components and instance initialization), use the above tag and **upload** all packages to the **`foo` bucket**.

3. The `prod_env_apply.sh` **checks** that the `OS_PROJECT_ID` environment variable contains the **ID** of the **production project**. As it is the case, it proceeds (otherwise it would have exit with error).

4. The `prod_env_apply.sh` asks **Terraform** to apply any change that have been made to the `core` module. To see what it has to do, Terraform will get its state from the `hidden_value` bucket (see [buckets.md](buckets.md)). As it is a fresh install, it will actually **create** all the resources of the `core` module.

5. The **configuration** of the deployment is done with some `.tfvars` file (as explained before) that contains some bucket access keys that are **specific** to the production environment and **secrets** that are not stored within the git project.

6. Some **Terraform** template files (`.tf`) get the values of some **OpenStack environment variables** from the current OpenStack configuration. This is needed in particular by the Nomad server that will cascade those values to the `worker-pool-management` Nomad job (for its task of creation/deletion of worker OpenStack instances).

7. Some **other configuration** needed by some instances are directly found from the **[config/main.env](../config/main.env)** which is directly included in some Terraform files.

8. All the resources are **created**, and some **other** existing **resources** not defined in the `core` module are used (like the ssh key to allow `tf-toto` to connect to all instances). There is also **dependencies** between resources, the **most important** ones are the IP addresses of the database and Nomad server instances which are needed by other instances.

9. **Each instance** to create is given the **`get_and_launch_instance_init.sh`** script and is configured to execute this script during the **first start** of the instance (see the cloud-init configuration). This script takes two **parameters**: the **name** of the instance and the **tag** to use for the package (`git-hash-220f73f3` in the example above). When executed it will **download** the init package with this tag, **extract** it and execute the **`init_instance.sh`** script. Just before executing this script, all necessary environment variables are set, in particular the **environment variables** that contain the **IP addresses** for the database and the Nomad server instances.

10. At this point, the **next actions** are carried on by the **`init_instance.sh`** script of each instance. This script is part of the init instance **package** that contains all the **files** that are needed to **prepare the instance** for its usage. As said earlier, some specific **configurations** are either given by the **`main.env`** file or by **environment variables** that were set in the cloud-init configuration just before the script execution.

11. If needed, the **software package** for the instance is **downloaded** using the same **tag** as the init instance package (i.e. `git-hash-220f73f3` in our example) and is **installed**. It is the case for the database, the dashboard and the worker instances.

12. The **Nomad server** is a **special** case. During its initialization, the script submits all the Nomad jobs that are needed by the system. And these **jobs** have to be **configured** with values that comes from some **files** (`main.env` for example), some **environment variables** (like the IP addresses) or some **commands** (for getting the secret of some EC2 credentials for example). During its initialization, all these configurations are injected in some **template** files for Nomad job files using the tool **`envsubst`**. The modified job files are then **submitted** to Nomad.

13. All those instances are configured to start every tool/components/etc. on the first start. So, once all **`init_instance.sh`** are executed with **success**, everything is **ready**. To check whether there has been some **error or not**, you can execute a command like `openstack console log show tf-tutu` and **check** that somewhere near the end there are two lines like:

    ```shell
    [  191.127493] cloud-init[1432]: [init_instance.sh] [2020-06-26 07:33:05] init_instance_finished_with_success
    [  191.129225] cloud-init[1432]: [init_instance.sh] [2020-06-26 07:33:05] done
    ```

14. At this point the complete **system** is up and **is starting**. At some point the database will be up, the Nomad server will have allocated the orchestrator jobs to the orchestrator instance, these jobs will download their docker image from the GitLab registry in `gitana-ext` from Magellium, the `tf-titi` is ready to be duplicated, etc.

# Update a deployed system

## Update the infrastructure

If there are some **modifications** in a Terraform template file (`.tf`) you can call the **apply** script and **Terraform** will **figure out** what needs to be changed, destroyed and/or created based on you modifications. It will describe this course of action precisely before **asking** you whether you really want to apply this modifications or not. Some modifications need to re-deploy nearly all the resources and some others might change just one existing resource (it is generally the case when you upscale the flavor of an instance for example).

## Install new component version

For now there is **no easy** and automatic way to deploy a new version of a
software component. And this may be **rarely needed**. Yet, if you need to, you
can do the following:  

If a new package with **tag** `git-hash-0e4af8d4` has been build and is uploaded in the appropriate path in the `foo` bucket.

* **Connect** to the **instance** where the software component is used.
* Go to the the **`/opt/csi`** directory.
* Locate the file **`get_component_package.sh`** in an existing `init_instance` directory.
* Go to this directory.
* Use this script to **download** and **unpack** the new package version:

``` shell
export CSI_BUCKET_NAME_INFRA=foo
./get_component_package.sh my_software_component git-hash-0e4af8d4
```

* **Install** the software components **manually**. This step depends on the component. Sometime it is only changing a symbolic link, sometime it is executing some script or patch, etc.

# Local system deployment

Some parts of the operational system can be deployed on your local machine for debugging purpose.

## Local database deployment

Move to the `components/database/` folder of the HR-S&I repository. Open a **terminal** there. Then, to build the **database docker image**, you can just run the following command, with sudo privilege :

```bash
docker build -t cosims-postgres .
```

Afterwards, you might **start the database** with the line below (with sudo privilege as well) :

```bash
docker-compose up
```

Once the database is running, you can **stop** it by pressing `ctrl + c` keys. Note that the **database content** will remain stored in memory, and if you enter the command to start it again, it will be **automatically loaded**. If you want to reset the database content, once it's stopped, run the follwoing command, also with sudo privilege :

```bash
docker-compose stop
```

## Local orchestrator service deployment

Details on the way to build orchestrator services docker images, and how to run them can be found in the different services docker documentation. Those files are located under `components/<service-name>/docker/readme.md` (creation service ReadMe file for instance : [readme.md](../components/job_creation/docker/readme.md)).

To build and run the services docker images, you should move to the **root** folder of your HR-S&I repository and open a **terminal** there. You can then run the commands specified in the services **docker readme files**. For instance, for the **creation service**, you can build the service docker image with the following command (to be run with sudo privilege) :

```bash
docker build --rm -t csi_job_creation -f components/job_creation/docker/Dockerfile .
```

Once the docker image build completed, you can run it with the line below, still for the **creation service** example (to be run with sudo privilege) :

```bash
docker run --rm \
    --env COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000 \
    --env CSI_SCIHUB_ACCOUNT_PASSWORD=dummy_value \
    --env CSI_SIP_DATA_BUCKET=dummy_value \
    --network=host \
    --name csi_job_creation_c \
    csi_job_creation
```

To **stop the service** execution, you can simply press `ctrl + c` keys.

## Local dashboard deployment

Refer to the dahsboard [readme.md](../components/dashboard/README.md) file.
