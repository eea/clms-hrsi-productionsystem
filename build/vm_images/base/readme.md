Create a base VM image that will be used for all instance with every necessary
tools and files.

It uses HashiCorp packer software and is best run on the OpenStack infra for
performance purposes.

# Create the VM image on the OpenStack registry

Be sure packer is installed, that your OpenStack env variables are set, then run:
``` shell
$ ./build.sh
```

# Update the VM base image referenced in the system

Once you have generated a new image, tagged with the latest git commit hash you 
should tell the HRSI system to use it.  
To do so you should update the `csi-base-git-xxxxxx` reference, to point to the 
proper image in the following **Terraform files** : 
- [main.tf](../../../deployment/terraform/admin/main.tf)
- [database_volume.tf](../../../deployment/terraform/cold/database_volume.tf)
- [nomad_server.tf](../../../deployment/terraform/core/nomad_server.tf)
- [orchestrator.tf](../../../deployment/terraform/core/orchestrator.tf)
- [worker.tf](../../../deployment/terraform/core/worker.tf)

Once it's done and committed, your next deployment should use your newly generated 
VM base image.
