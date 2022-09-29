data "openstack_images_image_v2" "database" {
  name = "image_name"
}

// This resource is the volume to use with the database instance. We use
// dedicated block storage volume for having a more persistent way of managing
// this critical part of the system.
resource "openstack_blockstorage_volume_v2" "database" {
  region = "RegionOne"
  name = "tf-tutu-volume"
  description = "Boot volume for the database instance"
  image_id = data.openstack_images_image_v2.database.id
  size = 32

  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}

output "database_volume_id" {
  value = openstack_blockstorage_volume_v2.database.id
}