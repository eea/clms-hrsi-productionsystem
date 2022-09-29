resource "openstack_compute_keypair_v2" "database_local_key" {
  name = "tf-tutu-local-keypair"

  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}

output "database_local_key" {
  value = {
    private = openstack_compute_keypair_v2.database_local_key.private_key
    public = openstack_compute_keypair_v2.database_local_key.public_key
  }
}