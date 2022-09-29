variable "admin_local_key" {
  type = object({
    private = string
    public = string
    name = string
  })
}

variable "csi_internal_ec2_credentials_access_key" {
  type = string
  description = "The OpenStack access key for the EC2 credentials to access the CSI internal buckets"
}

variable "csi_internal_ec2_credentials_secret_key" {
  type = string
  description = "The secret key for the EC2 credentials to access the CSI internal buckets"
}