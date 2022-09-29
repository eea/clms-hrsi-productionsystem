Create the infrastructure for the CoSIMS system.

Use Terraform tool from HashiCorp for IaC (infrastructure as code).

# Terraform modules

We have two kinds of resources. The ones that we can create and delete nearly at
will (like intances that host processings) and the ones that are critical and
need to be protected against accidental delete (like buckets that store results
files).

So we split these two kinds into 2 Terraform modules so we can easily delete
instances without deleting buckets.

# Terraform initialization

Before running any further terraform command, the `terraform init` one must be run 
to initialize the working directory.

# How to


Steps to create buckets:
* Check everything is OK with `terraform plan -target=module.buckets`.
* Actually create the infrastructure with `terraform apply -target=module.buckets`.
* Normally you don't have to delete the buckets. **Danger Zone** If you are
  absolutely sure you want to do this and you know what you are doing, remove
  the `prevent_destroy` attributes from the resources, then execute: `terraform
  destroy -target=module.buckets`

Steps to create instances:
* Go to `instances` directory and launch `prepare.sh`to create some files.
* Go back to main directory.
* Check everything is OK with `terraform plan -target=module.instances`.
* Actually create the infrastructure with `terraform apply -target=module.instances`.
* If you have to delete all the instances: `terraform destroy -target=module.instances`.

For now, this will also install all needed software and do all configuration
(services, env vars, etc.). Maybe this will be splitted in two step later: one
for infra and one for everything else.


# Admin server

A special instance is used to administrate the system. On can connect to
it using ssh and then can access all the system from the inside. To do this:
* First get the private key to connect to admin instance:
```shell
terraform output -json admin_private_key | jq -r '.private_key' > id_rsa
```
* Change the permission of this file: `chmod 600 id_rsa`
* Then connect to the server:
```shell
    ssh -i id_rsa eouser@45.130.28.134
```

# Database public SSH key

To get the public SSH key to allow the database instance to connect to other
host with ssh (for backups for example), do the following:

``` shell
$ terraform output -json database_public_key | jq -r '.' > csi_database_id_rsa.pub
```
