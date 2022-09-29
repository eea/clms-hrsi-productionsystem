// This resource contains the minimal stuff to download and launch a package
// that will do the actual instance initialisation. It use the standard
// cloud-init way of downloading and executing stuff.
//
// It uploads the basic script "get_and_launch_instance_init.sh" and execute it
// with the path to the init package in our bucket.
data "template_cloudinit_config" "nomad_server" {
  // It seems that OpenStack provider doesn't support base64. Disable it.
  gzip = false
  base64_encode = false

  // Execute some commands in the instance using cloud-init raw syntax (for more
  // information, see https://cloudinit.readthedocs.io/en/latest/topics/examples.html)
  part {
    content_type = "text/cloud-config"
    content = <<EOF
#cloud-config
write_files:
  - content: ${base64encode(
      file("${local.csi_source_dir}/common/get_and_launch_instance_init.sh")
    )}
    encoding: b64
    permissions: '0755'
    path: /opt/csi/bin/get_and_launch_instance_init.sh
  - content: ${base64encode(
      file("${local.csi_source_dir}/config/main.env")
    )}
    encoding: b64
    path: /opt/csi/config/main.env
  - content: |
      ${base64encode(templatefile("${path.module}/../admin/files/openstack_envs.sh", {
        OS_PASSWORD = data.external.openstack_env_vars.result.OS_PASSWORD
        OS_USERNAME = data.external.openstack_env_vars.result.OS_USERNAME
        OS_PROJECT_ID = data.external.openstack_env_vars.result.OS_PROJECT_ID
        OS_PROJECT_NAME = data.external.openstack_env_vars.result.OS_PROJECT_NAME
      }))}
    encoding: b64
    path: /opt/csi/config/openstack.sh
runcmd:
  - export CSI_HTTP_API_INSTANCE_IP=${openstack_compute_instance_v2.database.network.0.fixed_ip_v4}
  - export OS_PASSWORD="${data.external.openstack_env_vars.result.OS_PASSWORD}"
  - export CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY="${var.csi_internal_ec2_credentials_access_key}"
  - export CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY="${var.csi_internal_ec2_credentials_secret_key}"
  - export CSI_PRODUCT_PUBLICATION_ENDPOINT_PASSWORD="${var.csi_product_publication_endpoint_password}"
  - export CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_ACCESS_KEY="${var.csi_products_bucket_ec2_credentials_access_key}"
  - export CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_SECRET_KEY="${var.csi_products_bucket_ec2_credentials_secret_key}"
  - export CSI_SCIHUB_ACCOUNT_PASSWORD="${var.csi_scihub_account_password}"
  - export GITLAB_TOKEN_PASSWORD="${var.gitlab_token_password}"
  - /opt/csi/bin/get_and_launch_instance_init.sh nomad_server  ${local.init_packages_tag}
EOF
  }

}

resource "openstack_compute_secgroup_v2" "nomad" {
  name        = "tf-nomad"
  description = "Group to open Nomad port (for CLI and web UI)"

}

// The actual instance that will host the Nomad server.
resource "openstack_compute_instance_v2" "nomad_server" {
  name = "tf-tete"
  image_name = "image_name"
  flavor_name = "eo2.2xlarge"
  key_pair = var.admin_local_key_name
  security_groups = [
    "default",
    "allow_ping_ssh_rdp",
    "${openstack_compute_secgroup_v2.nomad.name}"
  ]
  user_data = data.template_cloudinit_config.nomad_server.rendered

  metadata = {
    prometheus_io_group = "nomad-server"
    prometheus_io_port = "9100"
    prometheus_io_scrape = "true"
  }

  network {
    name = "private_magellium"
  }
}

resource "openstack_networking_floatingip_v2" "nomad_server" {
  pool = "external_magellium"
}

resource "openstack_compute_floatingip_associate_v2" "nomad_server" {
  floating_ip = openstack_networking_floatingip_v2.nomad_server.address
  instance_id = openstack_compute_instance_v2.nomad_server.id
}

output "nomad_server_ip" {
  value = openstack_networking_floatingip_v2.nomad_server.address
}
