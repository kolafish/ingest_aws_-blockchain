output "driver_public_ip" {
  value = aws_instance.driver.public_ip
}

output "driver_private_ip" {
  value = aws_instance.driver.private_ip
}

output "es_public_ips" {
  value = aws_instance.es[*].public_ip
}

output "es_private_ips" {
  value = aws_instance.es[*].private_ip
}

output "es_urls" {
  value = join(",", [for ip in aws_instance.es[*].private_ip : "http://${ip}:9200"])
}

output "ssh_driver" {
  value = "ssh -i /Users/jin/Desktop/terraform-tici/master_key ubuntu@${aws_instance.driver.public_ip}"
}
