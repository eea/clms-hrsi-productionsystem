// This resource contains the minimal stuff to download and launch a package
// that will do the actual instance initialisation. It use the standard
// cloud-init way of downloading and executing stuff.
//
// It uploads the basic script "get_and_launch_instance_init.sh" and execute it
// with the path to the init package in our bucket.
data "template_cloudinit_config" "database" {
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
  - content: |
      ${base64encode(var.database_local_key.private)}
    encoding: b64
    path: /root/.ssh/csi_database_id_rsa
    permissions: '0600'
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
  %{ if fileexists("${path.module}/../database_patch.sql") }
  - content: ${base64encode(
      file("${path.module}/../database_patch.sql")
    )}
    encoding: b64
    path: /opt/csi/database_patch.sql
  %{ endif }
runcmd:
  - /opt/csi/bin/get_and_launch_instance_init.sh database  ${local.init_packages_tag}
EOF
  }

}

// To allow access the HTTP API provided by PostgREST, we need to open port
// 3000.
resource "openstack_compute_secgroup_v2" "postgrest" {
  name        = "hidden_value"
  description = "Group with PostrgREST port openned"

}

// The actual instance that will host the database.
resource "openstack_compute_instance_v2" "database" {
  name = "tf-tutu"
  flavor_name = "eo2.2xlarge"
  key_pair = var.admin_local_key_name
  security_groups = [
    "default",
    "allow_ping_ssh_rdp",
    "${openstack_compute_secgroup_v2.postgrest.name}"
  ]
  user_data = data.template_cloudinit_config.database.rendered

  metadata = {
    prometheus_io_group = "database"
    prometheus_io_port = "9100"
    prometheus_io_scrape = "true"
  }

  network {
    name = "private_magellium"
  }

  block_device {
    uuid = var.database_volume_id
    source_type = "volume"
    destination_type = "volume"
    boot_index = 0
    delete_on_termination = false
  }
}

resource "openstack_networking_floatingip_v2" "database" {
  pool = "external_magellium"
}

resource "openstack_compute_floatingip_associate_v2" "database" {
  floating_ip = openstack_networking_floatingip_v2.database.address
  instance_id = openstack_compute_instance_v2.database.id
}

output "database_ip" {
  value = openstack_networking_floatingip_v2.database.address
}
