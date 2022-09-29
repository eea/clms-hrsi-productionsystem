variable "database_volume_id" {
  type = string
}

variable "admin_local_key_name" {
  type = string
}

variable "csi_product_publication_endpoint_password" {
  type = string
}

variable "csi_products_bucket_ec2_credentials_access_key" {
  type = string
}

variable "csi_products_bucket_ec2_credentials_secret_key" {
  type = string
}

variable "csi_internal_ec2_credentials_access_key" {
  type = string
  description = "The OpenStack access key for the EC2 credentials to access the CSI internal buckets"
}

variable "csi_internal_ec2_credentials_secret_key" {
  type = string
  description = "The secret key for the EC2 credentials to access the CSI internal buckets"
}

variable "csi_scihub_account_password" {
  type = string
  description = "The password to connect to the ESA SciHub"
}

variable "gitlab_token_password" {
  type = string
  description = "The password for Nomad to access the gitlab repository"
}

variable "dashboard_ip" {
  type = string
}

variable "csi_packages_tag" {
  type = string
  description = "The tag to use for packages (for components, instance initialization...)"
}

variable "database_local_key" {
  type = object({
    private = string
    public = string
  })
}