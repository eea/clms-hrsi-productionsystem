variable "flavor_name" {
  type = string
  default = "eo1.medium"
  description = "The CloudFerro OpenStack flavor to use for the instance."
}

variable "image_name" {
  type = string
  default = "Ubuntu 18.04 LTS"
  description = "The CloudFerro OpenStack VM image to use for the instance."
}

variable "names_prefix" {
  type = string
  description = "The prefix to use for all resources that will be created. We recommand to use a prefix that makes it easy to identify you, like your username."
}