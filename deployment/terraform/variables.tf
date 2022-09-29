variable "csi_product_publication_endpoint_password" {
  type = string
  description = "The password to connect to the RabbitMQ endpoint for products publication"
}

variable "csi_products_bucket_ec2_credentials_access_key" {
  type = string
  description = "The access key to connect to the S3 bucket that store the CoSIMS product"
}

variable "csi_products_bucket_ec2_credentials_secret_key" {
  type = string
  description = "The secret key to connect to the S3 bucket that store the CoSIMS product"
}

variable "csi_packages_tag" {
  type = string
  description = "The tag to use for packages (for components, instance initialization...)"
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