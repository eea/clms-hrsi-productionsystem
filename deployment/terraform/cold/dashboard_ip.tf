resource "openstack_networking_floatingip_v2" "dashboard" {
  pool = "external_magellium"

  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}

output "dashboard_ip" {
  value = openstack_networking_floatingip_v2.dashboard.address
}