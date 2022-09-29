data "external" "openstack_ec2_credentials" {
    program = [
      "${path.module}/../../../common/get_openstack_ec2_credentials.sh",
      var.csi_internal_ec2_credentials_access_key
    ]
}

// Get some OpenStack information from OS_* env vars.
//
// The use of jq is a trick to access external env vars from terraform template.
// The string in the form:
//   "env | { MY_ENV_VAR }"
// is a jq command that output the following JSON:
//   {
//     "MY_ENV_VAR": "the value of MY_ENV_VAR when terraform is launch"
//   }
// And the data.external template takes this JSON and produces a resource that
// can be used like that:
//    data.external.openstack_env_vars.result.MY_ENV_VAR
data "external" "openstack_env_vars" {
    program = [
      "jq",
      "-n",
      "env | {OS_PASSWORD, OS_USERNAME, OS_PROJECT_ID, OS_PROJECT_NAME}"
    ]
}

// This cloud-init config is a bit complicated. It is because we need to deploy
// the SSH key pair that allows this instance to connect to all other instances.
// And this can only be done "manually". Even worse, in the "write_files"
// section below, as /home/eouser/.ssh, which is the actual final location, is
// not available at beginning of cloud-init, we put the files in a temporary
// place and then get them and configure them (rights, owner, etc.) in the
// "runcmd" part of cloud-init (at this point, /home/eouser exists).
data "template_cloudinit_config" "admin_config" {
  // It seems that OpenStack provider doesn't support base64. Disable it.
  gzip = false
  base64_encode = false

  // Upload some files into the instance using cloud-init raw syntax.
  part {
    content_type = "text/cloud-config"
    content = <<EOF
#cloud-config
write_files:
  - content: |
      ${base64encode(templatefile("${path.module}/../../../common/rclone_template.conf", {
        CSI_INTERNAL_EC2_CREDENTIALS_ACCESS_KEY = var.csi_internal_ec2_credentials_access_key
        CSI_INTERNAL_EC2_CREDENTIALS_SECRET_KEY = var.csi_internal_ec2_credentials_secret_key
      }))}
    encoding: b64
    path: /usr/local/bin/rclone.conf
  - content: |
      ${base64encode(templatefile("${path.module}/files/openstack_envs.sh", {
        OS_PASSWORD = data.external.openstack_env_vars.result.OS_PASSWORD
        OS_USERNAME = data.external.openstack_env_vars.result.OS_USERNAME
        OS_PROJECT_ID = data.external.openstack_env_vars.result.OS_PROJECT_ID
        OS_PROJECT_NAME = data.external.openstack_env_vars.result.OS_PROJECT_NAME
      }))}
    encoding: b64
    path: /tmp/home/eouser/openstack_envs.sh
  - content: |
      ${base64encode(var.admin_local_key.private)}
    encoding: b64
    path: /tmp/home/eouser/.ssh/id_rsa
    permissions: '0600'
  - content: |
      ${base64encode(var.admin_local_key.public)}
    encoding: b64
    path: /tmp/home/eouser/.ssh/id_rsa.pub
  - content: |
      {
        "experimental": true
      }
    path: /etc/docker/daemon.json
runcmd:
  - echo Copy some files into eouser home directory
  - chown -R eouser:eouser /tmp/home/eouser 
  - mv /tmp/home/eouser/openstack_envs.sh /etc/profile.d
  - mv /tmp/home/eouser/.ssh/* /home/eouser/.ssh
  - rm -rf /tmp/home
  - echo Be sure .ssh and its content have the appropriate owner
  - chown -R eouser:eouser /home/eouser/.ssh
  - echo Be sure .ssh dir has the appropriate permissions
  - chmod 700 /home/eouser/.ssh
EOF
  }

  part {
    filename = "instance_init.sh"
    content_type = "text/x-shellscript"
    content = file("${path.module}/files/instance_init.sh")
  }

}

// For now, we use a very unsecure way to allow the team to connect to the admin
// instance: we dynamically generate a SSH key pair which private key is stored
// in the Terraform state.
resource "openstack_compute_keypair_v2" "admin_external_key_pair" {
  name = "tf-toto-external-key"
}

resource "openstack_compute_secgroup_v2" "nomad" {
  name        = "tf-nomad-2"
  description = "Group to open Nomad port (for CLI and web UI)"

  rule {
    from_port   = 4646
    to_port     = 4646
    ip_protocol = "tcp"
    cidr        = "0.0.0.0/0"
  }
}

resource "openstack_compute_instance_v2" "admin_server" {
  name = "tf-toto"
  image_name = "image_name"
  flavor_name = "eo1.small"
  key_pair = openstack_compute_keypair_v2.admin_external_key_pair.name
  security_groups = [
    "default",
    "allow_ping_ssh_rdp",
    "${openstack_compute_secgroup_v2.nomad.name}"
  ]
  user_data = data.template_cloudinit_config.admin_config.rendered

  network {
    name = "private_magellium"
  }

  network {
    name = "eodata"
  }
}

resource "openstack_networking_floatingip_v2" "admin_floating_ip" {
  pool = "external_magellium"
}

resource "openstack_compute_floatingip_associate_v2" "admin_floating_ip_association" {
  floating_ip = openstack_networking_floatingip_v2.admin_floating_ip.address
  instance_id = openstack_compute_instance_v2.admin_server.id
}
