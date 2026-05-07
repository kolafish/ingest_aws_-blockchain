# ES Single-AZ 10GB Benchmark Infra

This Terraform stack provisions only the Elasticsearch production-style 10GB
rerun resources. It intentionally uses a separate state from `terraform-tici`
so applying this stack does not replace or start TiCI/TiDB instances.

Topology:

```text
3 x c5.xlarge ES nodes, 100GB gp3, 3000 IOPS, 125 MiB/s
1 x c5.xlarge driver, 100GB gp3, 3000 IOPS, 125 MiB/s
single AZ/subnet, public IPs for setup, private IPs for ES traffic
```

Prepare a local var file:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Run:

```bash
AWS_SHARED_CREDENTIALS_FILE=/Users/jin/Desktop/terraform-tici/.aws-empty-credentials \
AWS_SDK_LOAD_CONFIG=1 \
terraform init

AWS_SHARED_CREDENTIALS_FILE=/Users/jin/Desktop/terraform-tici/.aws-empty-credentials \
AWS_SDK_LOAD_CONFIG=1 \
terraform apply
```
