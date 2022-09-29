output "ip_address" {
  value = openstack_networking_floatingip_v2.admin_floating_ip.address
  description = "The IP address of the admin server instance."
}

output "external_private_key" {
  sensitive = true
  value = openstack_compute_keypair_v2.admin_external_key_pair.private_key
  description = "The private ssh key to connect to the admin instance for user 'eouser'. Store and use this key from any host that needs to connect to admin instance."
}
