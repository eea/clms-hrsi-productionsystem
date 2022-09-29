# Deployment

## Presentation

This directory contains everything that is needed to deploy the system and
manage a deployed system.


## Deployment environments

There are two deployment environments:
* The production environment, where the operational system is running. Of
  course, be very careful while applying changes or destroying anything on this
  environment.
* The test environment, which is dedicated to testing or dev operations.

Each environment is targeted to as specific OpenStack project in the CloudFerro
infrastructure:
* `foo` for production
* `bar` for test


## Configurations and tools

There is a dedicated list of files for each environment. Those files are
prefixed by the environment name, like the two files 'test_env.apply.sh' and
'prod_env.apply.sh' which do the same thing but each on its dedicated
environment.

Here is the list of files (with the test env as prefix, of course the same files
exist for the production env):
* `test_env_apply.sh` to apply some Terraform changes.
* `test_env_destroy.sh` to destroy some Terraform resource (use with care).
* `test_env_configuration.tfvars` which defines some configuration values which
  are safe to commit in git (i.e. no secrets).
* `test_env_secrets.tfvars` which defines the secrets that are needed for the
  configuration of the system and which are not safe to commit in git. The
  pattern `*_secrets.tfvars` has been add to `.gitignore` so that those files
  are never commit even accidentally. An empty example of this secret file can
  be found here
  [files_examples/test_env_secrets.tfvars](files_examples/test_env_secrets.tfvars).

For running the scripts, you need to have set the OpenStack configuration which
defines the values for all the `OS_*` en vars. This is generally done by
sourcing the appropriate `.sh` file. This OpenStack configuration **must** match
the expected OpenStack project for the given environment and this is checked at
the beginning of the scripts (by checking the value of `OS_PROJECT_ID`).


### Example

If you have just get a fresh git working directory, here are some steps to
deploy the test environment:
* Source the shell file to set the OpenStack account configuration (a file like
  `csi_test-openrc.sh`) and enter the appropriate password. A priori the user
  name to use is `os-automation-test`.
* Run the command `terraform init` in the directory [terraform](terraform). This
  has only to be done once to initialize the directory.
* Create the file `test_env_secrets.tfvars` using
  [files_examples/test_env_secrets.tfvars](files_examples/test_env_secrets.tfvars)
  as an example.
* Deploy the system with the script [test_env_apply.sh](test_env_apply.sh) by
  running the command (in this directory):

``` shell
$ ./test_env_apply.sh module.core
```

* Review the Terraform message about what it will create and if everything looks
  good answer yes.
* The system is being deployed.

By default the [test_env_apply.sh](test_env_apply.sh) will use the current
commit hash as a tag for the init instance packages (i.e. the `csi_packages_tag`
Terraform variable). To use another tag specfy it as the last parameter:

``` shell
$ ./test_env_apply.sh module.core my_tag
```

The script [test_env_destroy.sh](test_env_destroy.sh) is used to destroy some
Terraform resource(s).


### Patching the database during deployement

If you need to apply some changes to the database you can do so by defining a
SQL patch file that will be applied as a step of the initialization of the
database instance. For example, it can be useful for:
* Changing the default settings of the database. Like changing the system
  parameter for the name of the S&I software docker image to use.
* Applying some changes to an existing database during deployments that don't
  recreate the block storage volume of a previously deployed database instance.

To do so simply create the file
[terraform/database_patch.sql](terraform/database_patch.sql) with the SQL script
you want to execute. The file
[files_examples/database_patch.sql](files_examples/database_patch.sql) is an
example of such a script that sets several system parameters in the database.


## Admin instance

See [terraform/admin/readme.md](terraform/admin/readme.md) for more information
about the admin instance.

To deploy the admin instance use the command:

``` shell
./test_env_apply.sh module.admin
```

To connect to the admin instance you have to get the SSH private key and the IP
address that are stored as Terraform outputs of `module.admin`. These steps are
gathered in the script [ssh_connect_to_admin.sh](ssh_connect_to_admin.sh) which
thus can be used to connect to the admin instance.
