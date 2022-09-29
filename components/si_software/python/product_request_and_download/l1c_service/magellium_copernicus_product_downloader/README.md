# Tools to download S2 L1C and S3 SRAL products


######################
#developped by Magellium SAS
#author remi.jugier@magellium.fr
######################


## Using Magellium's service implemented on a Wekeo VM

### install rclone

Install rclone v1.50.2 which is a utility to communicate with S3 buckets, google drives, etc... i.e. cloud storages.

You can get rclone here : https://drive.google.com/file/d/1A4_2SulplKeTlwYaTa6QRfnLNJZ7gjEa/view?usp=sharing

Do not install through apt-get, because you will get an older version which will not work properly.

### configure rclone

Add the following lines to your $HOME/.config/rclone/rclone.conf file (create this file if it does not exist) : 
```
[distribute-external]
type = drive
client_id = hidden_value
client_secret = client_id
scope = drive
token = {"access_token":"hidden_value","token_type":"hidden_value","refresh_token":"hidden_value","expiry":"hidden_value"}
team_drive = hidden_value
```

### use s3_sral_downloader.py

You need a basic python3 environment with numpy and requests : `module load python` will suffice on CNES cluster.

Two examples are provided in the `run_example` file : one to download a specific product and the other to download all products within a time window.

You can use severall instances of this program in parallel, however please do not go over 4 as it will slow down the ongoing reprocessing work on the COSIMS project.


## Using creodias API

You need a basic python3 environment with requests : `module load python` will suffice on CNES cluster.

One example is provided in the `run_example` file.

Only a single instance of this program can be used at a given time ; adding a new instance will make the ongoing instances fail.

WARNING : if the download takes more than 600 seconds, it will fail because the token will have expired. For this reason, this tool cannot be used to download S3 L1A on CNES cluster (too slow).

A creodias account has been created for the occasion for convenience, but if you create your own free account it will work of course...
