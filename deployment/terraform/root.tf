module "buckets" {
  source = "./buckets"
}

module "core" {
  source = "./core"
  database_volume_id = module.cold.database_volume_id
  admin_local_key_name = module.cold.admin_local_key.name
  csi_product_publication_endpoint_password = var.csi_product_publication_endpoint_password
  csi_products_bucket_ec2_credentials_access_key = var.csi_products_bucket_ec2_credentials_access_key
  csi_products_bucket_ec2_credentials_secret_key = var.csi_products_bucket_ec2_credentials_secret_key
  csi_internal_ec2_credentials_access_key = var.csi_internal_ec2_credentials_access_key
  csi_internal_ec2_credentials_secret_key = var.csi_internal_ec2_credentials_secret_key
  csi_scihub_account_password = var.csi_scihub_account_password
  gitlab_token_password = var.gitlab_token_password
  dashboard_ip = module.cold.dashboard_ip
  csi_packages_tag = var.csi_packages_tag
  database_local_key = module.cold.database_local_key
}

module "cold" {
  source = "./cold"
}

module "admin" {
  source = "./admin"
  admin_local_key = module.cold.admin_local_key
  csi_internal_ec2_credentials_access_key = var.csi_internal_ec2_credentials_access_key
  csi_internal_ec2_credentials_secret_key = var.csi_internal_ec2_credentials_secret_key
}

output "admin_ip_address" {
  value = module.admin.ip_address
  description = "The external IP address of the admin instance."
}

output "admin_external_private_key" {
  sensitive = true
  value = module.admin.external_private_key
  description = "The private ssh key to connect to the admin instance for user 'eouser'. Store and use this key from any host that needs to connect to admin instance."
}

output "dashboard_ip" {
  value = module.cold.dashboard_ip
  description = "The external IP address of the admin instance."
}

output "database_public_key" {
  value = module.cold.database_local_key.public
  description = "The public ssh key of the database instance."
}
