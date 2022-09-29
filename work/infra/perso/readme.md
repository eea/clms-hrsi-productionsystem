Create a temporary OpenStack instance in the DIAS for personal use. The public
ssh key of the current user is uploaded in the new instance to allow the user to
connect without a password.

## Prerequisites

Terraform must be installed and all `OS_` environment variables must be set.

## How to

### Initialisation

Initialize Terraform (execute it once):

``` shell
terraform init
```


### Creation

Create the instance with:

``` shell
terraform apply
```

The apply will ask you for a prefix to use for all resources names and it will
display what terraform will create and it ask for confirmation. Answer yes. At
the end of the creation it displays the IP address of the instance:

```
Apply complete! Resources: 4 added, 0 changed, 0 destroyed.

Outputs:

ip_address = 45.130.28.246
```

To connect to the instance:

``` shell
ssh eouser@45.130.28.246
```


### Customisation

If you want to use another flavor or VM image, or if you don't want to enter the
name prefix each time you execute apply, you can either edit the
[variables.tf](variables.tf) file or pass the value to the apply command:

``` shell
terraform apply \
  -var names_prefix=foo \
  -var flavor_name=eo2.large \
  -var image_name=image_name
```


### Destruction

When you don't need the instance any more don't forget to destroy it:

``` shell
terraform destroy
```

