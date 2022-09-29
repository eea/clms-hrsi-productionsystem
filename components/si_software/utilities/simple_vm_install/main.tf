// The key pair that will be used for the instance. We use the current user's
// one on th elocal file system.
resource "openstack_compute_keypair_v2" "my_keypair" {
  name = "${var.names_prefix}-keypair"
  // Use the default file for the public ssh key.
  public_key = file("~/.ssh/id_rsa.pub")
}

// This is the actual VM specification.
resource "openstack_compute_instance_v2" "my_instance" {
  name = "${var.names_prefix}-instance"
  image_name = var.image_name
  flavor_name = var.flavor_name
  key_pair = openstack_compute_keypair_v2.my_keypair.name
  security_groups = [
    "default",
    "allow_ping_ssh_rdp",
    "allow_http_https"
  ]

  network {
    name = "private_magellium"
  }

  network {
    name = "eodata"
  }
}

// The external UP address that one can use to connect to the instance.
resource "openstack_networking_floatingip_v2" "my_floating_ip" {
  pool = "external_magellium"
}

// We need to explicitly associate the IP to the instance.
resource "openstack_compute_floatingip_associate_v2" "my_floating_ip_association" {
  floating_ip = openstack_networking_floatingip_v2.my_floating_ip.address
  instance_id = openstack_compute_instance_v2.my_instance.id
}
