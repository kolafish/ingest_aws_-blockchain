locals {
  common_user_data = <<-EOT
    #!/usr/bin/env bash
    set -euxo pipefail
    echo 'vm.max_map_count=262144' >/etc/sysctl.d/99-elasticsearch.conf
    sysctl -w vm.max_map_count=262144
    mkdir -p /home/ubuntu/elasticsearch-prod10g /home/ubuntu/bench
    chown -R ubuntu:ubuntu /home/ubuntu/elasticsearch-prod10g /home/ubuntu/bench
  EOT
}

resource "aws_security_group" "es" {
  name_prefix = "${var.namespace}-"
  vpc_id      = var.vpc_id

  ingress {
    description = "SSH setup access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Cluster-internal traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "es" {
  count = length(var.es_private_ips)

  ami                         = var.ami_id
  instance_type               = var.instance_type
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.es.id]
  subnet_id                   = var.subnet_id
  associate_public_ip_address = true
  private_ip                  = var.es_private_ips[count.index]

  root_block_device {
    volume_size           = var.volume_size_gb
    delete_on_termination = true
    volume_type           = "gp3"
    iops                  = var.volume_iops
    throughput            = var.volume_throughput
  }

  tags = {
    Name = "${var.namespace}-es-${count.index + 1}"
    Role = "es"
  }

  user_data = local.common_user_data
}

resource "aws_instance" "driver" {
  ami                         = var.ami_id
  instance_type               = var.instance_type
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.es.id]
  subnet_id                   = var.subnet_id
  associate_public_ip_address = true
  private_ip                  = var.driver_private_ip

  root_block_device {
    volume_size           = var.volume_size_gb
    delete_on_termination = true
    volume_type           = "gp3"
    iops                  = var.volume_iops
    throughput            = var.volume_throughput
  }

  tags = {
    Name = "${var.namespace}-driver"
    Role = "driver"
  }

  user_data = local.common_user_data
}
