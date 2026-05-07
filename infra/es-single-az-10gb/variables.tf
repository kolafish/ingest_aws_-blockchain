variable "aws_profile" {
  type    = string
  default = "default"
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "namespace" {
  type    = string
  default = "es-prod10g-bench"
}

variable "ami_id" {
  type        = string
  description = "Ubuntu AMI ID in the selected region."
  default     = "ami-003e5556ddc999e13"
}

variable "vpc_id" {
  type        = string
  description = "Existing VPC ID."
}

variable "subnet_id" {
  type        = string
  description = "Existing subnet ID."
}

variable "key_name" {
  type        = string
  description = "Existing EC2 key pair name."
}

variable "instance_type" {
  type    = string
  default = "c5.xlarge"
}

variable "volume_size_gb" {
  type    = number
  default = 100
}

variable "volume_iops" {
  type    = number
  default = 3000
}

variable "volume_throughput" {
  type    = number
  default = 125
}

variable "es_private_ips" {
  type    = list(string)
  default = ["172.31.21.1", "172.31.21.2", "172.31.21.3"]
}

variable "driver_private_ip" {
  type    = string
  default = "172.31.22.1"
}
