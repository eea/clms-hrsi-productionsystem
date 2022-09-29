// This resource contains the minimal stuff to download and launch a package
// that will do the actual instance initialisation. It use the standard
// cloud-init way of downloading and executing stuff.
//
// It uploads the basic script "get_and_launch_instance_init.sh" and execute it
// with the path to the init package in our bucket.
data "template_cloudinit_config" "worker" {
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
  - export CSI_HTTP_API_INSTANCE_IP=${openstack_compute_instance_v2.database.network.0.fixed_ip_v4}
  - export CSI_NOMAD_SERVER_IP=${openstack_compute_instance_v2.nomad_server.network.0.fixed_ip_v4}
  - /opt/csi/bin/get_and_launch_instance_init.sh worker ${local.init_packages_tag}
EOF
  }

}

// The actual instance that will host a worker.
resource "openstack_compute_instance_v2" "worker_template" {
  name = "tf-titi"
  image_name = "image_name"
  // Choose the smallest flavor. Because this template is not meant to actually
  // receive Nomad allocation. But for some reason (after a manual restart for
  // example)

  // This template is not meant to receive Nomad job allocation. But as the
  // template is used to produce a VM image that will be ready to start the
  // Nomad agent, this template instance might be started with a running Nomad
  // agent by error (by default the systemd service is not started but a reboot
  // of the template instance will actually run the Nomad agent). If this
  // happens, and to be sure Nomad doesn't choose this agent during its search
  // we choose the smaller falvor possible and this flavor hasn't sufficient
  // resources (in particular for the RAM) so Nomad won't choose it.
  flavor_name = "eo1.xsmall"

  key_pair = var.admin_local_key_name
  security_groups = [
    "default",
    "allow_ping_ssh_rdp"
  ]
  user_data = data.template_cloudinit_config.worker.rendered

  metadata = {
    prometheus_io_group = "worker-template"
    prometheus_io_port = "9100"
    prometheus_io_scrape = "true"
  }

  network {
    name = "private_magellium"
  }

  network {
    name = "eodata_magellium"
  }
}