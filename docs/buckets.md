# Buckets

Here is the list of Object Storage buckets and their usage.

All the bucket resources are managed by Terraform except when noted. Buckets
that are managed by Terraform have their name prefixed with `tf-` except when
noted.

## Diagram

``` mermaid
graph TD
  subgraph "S&I processing buckets"
  HRSI
  hidden_value
  hidden_value
  hidden_value
  end
```

``` mermaid
graph TD
  subgraph "general buckets"
  foo
  hidden_value
  work
  public
  end
```

``` mermaid
graph TD
  subgraph "Terraform state buckets"
  hidden_value
  hidden_value-archive
  end
```

## S&I processings data buckets

### Products

Name of the bucket: `HRSI`

This is the bucket where the HR-S&I products are stored and accessed by the
DIAS.

This bucket is managed by Terraform, even if its name doesn't start with `tf-`.
It is because its name is shared outside the HR-S&I system and we don't want to expose this implementation detail.

Hierarchy:

* `CLMS/Pan-European/High_Resolution_Layers/FSC/<year>/<month>/<day>/FSC_<product_id>`
* The same for RLIE.

If one wants to restart the system from scratch, and also empty the DIAS catalog,
the content of this bucket should be removed. It can be done by following the
instructions detailled in the [dedicated section](#bucket-content-reset) of this document.

### Results

Name of the bucket: `hidden_value`

This bucket stores the outputs at the end of each S&I treatment. The contents of the bucket are not intended to remain forever. At the beginning of the production phase we can keep some of it for debugging purposes. However, a clean-up strategy must be put in place afterwards (such as deleting content older than one month).

The files in this bucket include:

* Processing logs from S&I software, Maja, Worker
* Output files that are not stored in the product bucket.

Hierarchy:

* `jobs/<job_id>` contains a copy of the `/opt/csi/work/jobs/<job_id>` directory but input files, products and temporary directory.

### Auxiliary

Name of the bucket: `hidden_value`

This bucket stores auxiliary data needed by S&I processings. Stored at the S2
tile level. All data are fairly stable: they are generated once and there are
few chance we have to generate them again.

This includes:

* DEM. All files need around 250 GB.
* TCD.
* EU hydro river and water mask
* HRL QC flags (for RLIE products QC layer)

Hierarchy:
|Path|File example|
|:-|-:|
|`/eu_dem/<S2_tile_id>`|`S2__TEST_AUX_REFDE2_T<S2_tile_id>_0001`|
|`/tree_cover_density/<S2_tile_id>`|`TCD_<S2_tile_id>.tif`|
|`/eu_hydro/raster/20m/<S2_tile_id>`|`eu_hydro_20m_<S2_tile_id>.tif`|
|`/eu_hydro/shapefile/<S2_tile_id>`|`eu_hydro_<S2_tile_id>.shp`|
|`/hrl_qc_flags/<S2_tile_id>`|`hrl_qc_flags_<S2_tile_id>.tif`|
|`/csi_aux/csi_aux_<S2_tile_id>.tar`|Contains tar of the previous aux data on an S2 tile basis|

### Data

Name of the bucket: `hidden_value`

This bucket stores data produced and needed between successive processings on a
given tile. It is mainly S2 L2A reference metadata that needed to be updated
after a processing and used as input for the next processing.

In case of backward Maja mode we need to temporarily store alternative L2A
metadata for reprocessing of a small list of products. These metadata are also
stored here.

Hierarchy:

* `<S2_tile id>/L2A/reference/<date>/<mode>` One L2A metadata by date.
* `<S2_tile id>/L2A/temporary/backward_<first_date_of_the_list>/<date>`

If one want to restart the system from scratch, and also empty the DIAS catalog,
the content of this bucket can be removed to free some storage space. It can be
done by following the instructions detailled in the [dedicated section](#bucket-content-reset) section
of this document.

## General purpose buckets

* `foo` Contains artifacts used during build, deploy, etc. Like docker
  images, data or installers.
* `hidden_value` Place to store backups (database...).
* `public`: A general purpose public bucket for access without credentials. For
  now, it is not sure whether this bucket will be actually used or not. Moreover Terraform can't manage a public bucket. So, we don't create it for now. If it is needed, it will be created manually with public access enabled.
* `work`: A general and informal bucket for operations by developers that need a
  bucket. As this bucket is not actually used by the system, it is not managed
  by Terraform.

## Terraform state buckets

Terraform needs to store the infrastructure state at a central location. We
chose to store it in a Storage Bucket of the OpenStack infrastructure. During
the first connection attempt to this distant storage, terraform creates two
buckets it will use thereafter. The buckets are:

* `hidden_value` to store the current infrastructure state file and lock
  files.
* ???

See the Terraform documentation [???] for more information.

## Bucket content reset

Three methods which can be used to empty a bucket will be discussed in this
section, with their pros and cons.

### OpenStack dashboard

The only solution discussed using an HMI. It implies to log in the [OpenStack
dashboard](https://cf2.cloudferro.com/auth/login/?next=/) with an account
which has access to the bucket we want to reset. The main advantage of this
solution is that, thanks to the HMI, it's relatively safe to use. Indeed, it's
easy to check that we are logged with the correct account and actually working on the
proper project to empty the right bucket.

| pros       | cons     |
| :------------- | -----------: |
| Safe to use | Slowest solution to delete content |
| Easy to handle | Can only delete one file/folder at a time |
| Can delete specific file/folder/sub-folder |  |

### Rclone command

The first command oriented method to be discussed is `rclone`. It offers a good deletion control, just like with the HMI and it's easy to delete a specific file/folder within a bucket. However, a `rclone.config` file must be set prior to its use, and the buckets which we will be accessible depend on the pair of access/secret keys specified in it. An example of this file can be found under `common/rclone_template.conf`.

Rclone commands to use :

|Code|Use|
|:-|-:|
|`rclone lsd foo` or `rclone lsd fooBUCKET_NAME` |to list all the buckets, or all the directories within a bucket.|
|`rclone lsl fooBUCKET_NAME` |to list all files within a bucket.|
|`rclone delete fooBUCKET_NAME/FOLDER_NAME`| to delete a specific folder within a bucket. The folder name can be omitted to delete the whole bucket. In addition some options can be passed to add selection criteria.|
|`rclone purge fooBUCKET_NAME/FOLDER_NAME` |to delete a specific folder within a bucket. The folder name can be omitted to delete the whole bucket. Additional selection criteria can't be used.|

| pros       | cons     |
| :------------- | -----------: |
| faster than HMI | Remains slow for big amount of data |
| Can delete specific file/folder/sub-folder | Requires a config file to be set |
| Can delete several folders with one command using `delete` with selection criteria | The config file can be source of misunderstanding if not updated between two usages |

### Swift command

The fastest solution is to be presented here. It requires some OpenStack environment variables to be set. They can be sourced from shell files downloaded on OpenStack dashboard (RC V3) onced logged in with the appropriate user. This user must have access to the bucket we want to clean. Note that the deletion speed seems to be improved if the command is run from a VM, on the DIAS. The VM's CPU and memory resources might also affect it. Once the environment is set, the following commands can be used :

|Code|Use|
|:-|-:|
|`swift list`|to list all the buckets.|
|`swift delete BUCKET_NAME --quiet`|to delete a specific bucket. The `--quiet` option disables standard output to improve the deletion speed.|

| pros       | cons     |
| :------------- | -----------: |
| Fastest solution | Can only delete buckets, not specific content in it |
| Easy to use | Easy to make mistakes if we didn't source the right OpenStack shell file |
|  | To run this command on a DIAS VM, the OpenStack shell file needs to be copied on it |
