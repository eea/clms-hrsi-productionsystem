locals {
  // The name of the bucket (or container name) in the OpenStack Object Storage
  // space for CSI.
  bucket = "foo"

  // Path to the root dir of the CSI source code.
  csi_source_dir = "${path.module}/../../.."

  init_packages_tag = var.csi_packages_tag
}