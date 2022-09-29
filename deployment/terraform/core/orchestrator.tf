// This resource contains the minimal stuff to download and launch a package
// that will do the actual instance initialisation. It use the standard
// cloud-init way of downloading and executing stuff.
//
// It uploads the basic script "get_and_launch_instance_init.sh" and execute it
// with the path to the init package in our bucket.
data "template_cloudinit_config" "orchestrator" {
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
runcmd:
  - export CSI_NOMAD_SERVER_IP=${openstack_compute_instance_v2.nomad_server.network.0.fixed_ip_v4}
  - /opt/csi/bin/get_and_launch_instance_init.sh orchestrator  ${local.init_packages_tag}
EOF
  }

}

// The actual instance that will host the orchestrator.
resource "openstack_compute_instance_v2" "orchestrator" {
  name = "tf-tata"
  image_name = "image_name"
  flavor_name = "eo1.large"
  key_pair = var.admin_local_key_name
  security_groups = [
    "default",
    "allow_ping_ssh_rdp"
  ]
  user_data = data.template_cloudinit_config.orchestrator.rendered

  metadata = {
    prometheus_io_group = "orchestrator"
    prometheus_io_port = "9100"
    prometheus_io_scrape = "true"
  }

  network {
    name = "private_magellium"
  }
}
