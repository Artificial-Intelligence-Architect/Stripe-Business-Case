# Terraform — AWS Infrastructure

Infrastructure-as-Code for Stripe data platform on AWS.

## Components
- VPC + Networking (multi-AZ)
- EKS Cluster (3-10 nodes)
- RDS Aurora PostgreSQL (3 nodes)
- ElastiCache Redis (3 nodes)
- S3 Data Lake
- KMS Encryption
- Security Groups & IAM

## Deploy
```bash
terraform init
terraform plan -var="environment=prod"
terraform apply -var="environment=prod"
```

## Timing
- Total deployment: ~20-30 minutes
- VPC: 5-10 min
- EKS: 10-15 min
- RDS: 5-10 min

See main.tf for full configuration details.
