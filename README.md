# payment-ecosystem

### Install AWS-CLI
```sh
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf aws awscliv2.zip
```

### Install Terraform
```sh
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common

wget -O- https://apt.releases.hashicorp.com/gpg | \
gpg --dearmor | \
sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null

gpg --no-default-keyring \
--keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg \
--fingerprint

echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(grep -oP '(?<=UBUNTU_CODENAME=).*' /etc/os-release || lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list

sudo apt update

sudo apt-get install terraform

terraform -help
terraform -version
```

### Commands
```sh
##### TEST APPLICATION VIA DOCKER-COMPOSE
docker compose up -d --build
docker logs p2a-service --tail 25
docker compose down

##### TEST APPLICATION LOCALLY
curl -i -X POST http://localhost:8080/webhooks/p2a/status-updates -H "Content-Type: application/json" -d '{"payoutId":"pay_987654321","status":"PAYOUT_APPROVED","reason":null}'

##### CHECK MESSAGES USING KAFKA UI
http://localhost:8090

##### CONFIGURE AWS CREDENTIALS (ACCESS KEY, SECRET KEY, REGION) AND VERIFY ACTIVE IDENTITY
aws configure
aws sts get-caller-identity

##### CREATE ECR REPO
cd ~/IdeaProjects/payment-ecosystem/infra
terraform init
terraform apply -target=aws_ecr_repository.p2a -auto-approve

##### LOGIN IN ECR, CREATE IMAGE AND PUSH IT TO AWS
ECR_URL=$(terraform output -raw ecr_repository_url)

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$ECR_URL"

cd ~/IdeaProjects/payment-ecosystem/p2a
docker build -t "$ECR_URL:latest" .
docker push "$ECR_URL:latest"

##### PROVISION THE REST OF INFRA
cd ~/IdeaProjects/payment-ecosystem/infra
terraform apply -auto-approve

##### WAIT THE CONTAINER TO BECOME HEALTHY
aws elbv2 describe-target-health \
  --target-group-arn $(aws elbv2 describe-target-groups --names payment-ecosystem-p2a-tg --query 'TargetGroups[0].TargetGroupArn' --output text)

##### CALL AWS PUBLIC ENDPOINT
WEBHOOK_URL=$(terraform output -raw webhook_url)
curl -i -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -d '{"payoutId":"pay_aws_001","status":"PAYOUT_APPROVED","reason":null}'

##### CONFIRM DELIVERY IN MSK THROUGH CLOUDWATCH LOGS
aws logs tail /ecs/payment-ecosystem/p2a --follow

##### REMOVE INFRA
cd ~/IdeaProjects/payment-ecosystem/infra
terraform destroy -auto-approve

##### DOUBLE-CHECK (SHOULD RETURN NO RESOURCES)
terraform state list
```
