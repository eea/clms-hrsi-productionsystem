# Technologies

The HR-S&I system is deployed on a cloud platform that is managed by
**OpenStack**, which is the only external dependency. For now, the cloud
provider is **CloudFerror**, but only few parts explicitly rely on CloudFerro
(it is the case for input data access).

Most of the infrastructure is defined and actually managed using **Terraform**.

In rare cases some resources might have been created or are managed manually
with the **Horizon** dashboard (Horizon is the name of the OpenStack dashboard).

Some **dynamic** resource management (to scale up and down the workers, for
example) don't use Terraform but directly use OpenStack.

# Infrastructure environments

There are two infrastructure environments, one for production and one for
testing and development. Each one is managed in a specific OpenStack project.

## Production environment

* OpenStack project: `hidden_value`
* OpenStack user for automation tasks (like building and deployment):
  `os-automation`

For now it is not the case, but it would be more safe to restrict the access to
this environment to only few accounts/persons and maybe from a specific machine.
This can avoid some mistakes that can modifies or destroys things in this
production environment.

## Test environment

* OpenStack project: `hidden_value`
* OpenStack user for automation tasks (like building and deployment):
  `os-automation-test`

# Naming

Here is the naming convention for the OpenStack resources:

* The **`tf-`** prefix identifies resources that are automatically managed by
  **Terraform**, i.e. mainly from Terraform template files in the directory
  [deployment/terraform](../deployment/terraform)
* The **`os-`** prefix identifies resources that are automatically managed by
  OpenStack, i.e. we execute **OpenStack** request from our softwares (mainly
  using the OpenStack Python SDK).
* **No prefix** for resources that are created **manually** either from Horizon
  or from command line tools.
* Use your **hexagram** as a prefix for any **personal** resources, like a
  personal instance for working from the inside of the system. Example:
  `foo-instance`.

# Buckets and credentials management

See [buckets.md](buckets.md) for more information about buckets and their usage.

## Amazon EC2/S3 compatible credentials

The access to the buckets are managed with credentials. As the OpenStack buckets
follow the Amazon S3 standard, those credentials can be used by any S3
compatible tool (we have chosen to use the rclone tool). The credentials are
managed with the **OpenStack command line tool**. For example to get the list of
credentials access keys:

```shell
$ openstack ec2 credentials list -c Access
+----------------------------------+
| Access                           |
+----------------------------------+
|**********************************|
+----------------------------------+
```

We use similar commands to create or delete credentials:

```shell
openstack ec2 credentials create
openstack ec2 credentials delete <access_key>
```

## Management

For now, we **don't know** how to set up **read only access** to buckets with
the credentials. So it is very **important** to understand that **anyone** that
use the credentials **can write/delete** the content of the bucket. So the
credentials have to be **carefully** managed and shared.

Here are the main bucket access usages in the system.

### Our internal buckets

We need to have read and write access to all our internal buckets. To store and
access the software artifacts (packages, S&I docker images...), the auxiliary
data, the jobs logs, etc. The credentials for these access are stored in the
following environment variables:

```shell
CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY
CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY
```

These buckets are used by:

* The **build and deployment** steps, when creating the artifacts and using
  them.
* Some of the software component deployed on the same OpenStack project, and it
  is mainly the **`worker` component**.
* By the **operation** team to **investigate** the behavior of a running system
  (to access log, saved working directories, etc.).
* The **dashboard**, which displays the logs of the processings that are stored
  in a bucket
* There is **no external** usage for these buckets.

These buckets are created on the same OpenStack project as the rest of the
system.

Each **environment** (production and test) has its own set of internal buckets
whith its own credentials.

### Bucket to store the HR-S&I products

This is the bucket were the products of the processings are stored. It is the
**most important one** (at least for the one used in production).

The credentials for these access are store in the following environment
variables:

```shell
CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_ACCESS_KEY
CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_SECRET_KEY
```

This bucket are used by:

* The **`worker` component** which uploads the product just after the S&I
  software has finished producing them.
* CloudFerro to access the HR-S&I products for their catalog. This is the only
  **external** usage for this bucket.

The **production environment** use the official product bucket which name is
`HRSI` and which is the main resource of a dedicated project which name is
`csi_products_buckets`.

The **test environment** hosts internally its own `HRSI` bucket and it is **not
shared** with CloudFerro.
