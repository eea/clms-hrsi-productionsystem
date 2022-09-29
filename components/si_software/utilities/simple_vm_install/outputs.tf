output "ip_address" {
  value = openstack_networking_floatingip_v2.my_floating_ip.address
  description = "The IP address of the instance"
}
