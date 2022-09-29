# OpenStack tools and configuration

To install the OpenStack CLI tool:

``` shell
pip install python-openstackclient
pip install python-heatclient
```

## Set the OpenStack credentials for you shell session

Get the credentials from the Horizon dashboard of CloudFerro:

* Connect to the dashboard (<https://cf2.cloudferro.com>) with your login and
  password.
* Select the project you want to work on.
* Go to <https://cf2.cloudferro.com/project/api_access/>
* Click on "Download OpenStack RC file" and download the "v3" file. Its name is
  something like that: "foo.sh"
* Source it to set some environment variables (all beginning with the OpenStack
  prefix `OS_`) and enter your password:

``` shell
source './foo.sh'
```

You are ready to use the `openstack` command. Example:

``` shell
$ openstack server list --column Name
+-----------------+
| Name            |
+-----------------+
| tf-toto         |
| tf-tata         |
+-----------------+
```

# Nomad, Packer and Terroform from HashiCorp

Installing those tools is as easy as  downloading the zip file, uncrompress it
and put it single file somewhere in your path. Example with terraform:

``` shell
terraform_version="0.12.24"
curl --silent --remote-name https://releases.hashicorp.com/terraform/${terraform_version}/terraform_${terraform_version}_linux_amd64.zip
unzip terraform_${terraform_version}_linux_amd64.zip
# If ~/local/bin is in your path
mv terraform ~/local/bin/
rm terraform_${terraform_version}_linux_amd64.zip
```

# Rclone

Rclone is the tool of choice to access Object Storage (S3 compatible buckets).

## Installing rclone

Installing is as easy as downloading the zip file, uncrompress it and put it
single file somewhere in your path:

``` shell
curl --silent --remote-name  https://downloads.rclone.org/v1.50.2/rclone-v1.50.2-linux-amd64.zip                         
unzip rclone-v1.50.2-linux-amd64.zip
# If ~/local/bin is in your path
mv rclone-v1.50.2-linux-amd64/rclone ~/local/bin/
rm rclone-v1.50.2-linux-amd64.zip
rm -rf rclone-v1.50.2-linux-amd64
```

## Configuring and using rclone

To use rclone with some S3 compatible storage you have to configure it with the
access URL and credentials. For CloudFerro buckets here is a example of
configuration file:

``` rclone
[eodata]
type = s3
env_auth = false
access_key_id = access
secret_access_key = access
endpoint = http://data.cloudferro.com:80
location_constraint = RegionOne

[foo]
type = s3
env_auth = false
access_key_id = <<< your EC2 credentials access key >>>
secret_access_key = <<< your EC2 credentials secret key >>>
endpoint = https://s3.waw2-1.cloudferro.com
location_constraint = RegionOne
```

You can get the EC2 credentials for CloudFerro with the command:

``` shell
$ openstack ec2 credentials list --column Access --column Secret
+----------------------------------+----------------------------------+
| Access                           | Secret                           |
+----------------------------------+----------------------------------+
| *********************************|**********************************|
+----------------------------------+----------------------------------+
```

Then you can access the buckets with commands like:

``` shell
$ rclone lsd eodata:
          -1 2017-11-15 10:40:52        -1 DIAS
          -1 2017-11-15 10:40:52        -1 EOCLOUD
          -1 2017-11-15 10:40:52        -1 EODATA
$ rclone lsd bar:
          -1 2020-01-29 21:35:21        -1 tf-titi
          -1 2020-01-29 21:35:21        -1 tf-tata
          -1 2020-01-29 21:35:21        -1 tf-toto
          -1 2020-01-29 21:35:21        -1 tf-tutu
          -1 2020-01-29 21:35:21        -1 tf-tete
          -1 2020-01-29 20:39:35        -1 tf-tyty
```

To copy some file to or from a bucket, you can use the copy command:

``` shell
$ rclone copy \
    eodata:eodata:EODATA/Sentinel-2/MSI/L1C/etc \
    ./local_dir_for_l1c \
```

To get the content of one file and send it to stdout:

``` shell
$ rclone cat foo:path_to_file.txt
the content of the file
$
```

You can use the cat command to download some tar file and extract it on the
fly:

``` shell
$ rclone cat \
    foo:path_to_archive.tar \
    | tar xf -
```

Or to download a docker image and load it in your local docker configuration:

``` shell
$ rclone cat \
    foo:path_to_docker_image \
    | docker load
```
