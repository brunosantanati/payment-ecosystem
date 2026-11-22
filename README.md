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

##### PROVISION SQS + DLQ + CLOUDWATCH ALARM

cd ~/IdeaProjects/payment-ecosystem/infra
terraform apply \
  -target=aws_sqs_queue.payout_commands_dlq \
  -target=aws_sqs_queue.payout_commands \
  -target=aws_sqs_queue_redrive_allow_policy.payout_commands_dlq_allow \
  -target=aws_iam_policy.payout_sqs_dev_policy \
  -target=aws_cloudwatch_metric_alarm.payout_dlq_not_empty \
  -auto-approve
  
##### SEND A MESSAGE TO SQS AND INSPECT IT

QUEUE_URL=$(cd ~/IdeaProjects/payment-ecosystem/infra && terraform output -raw payout_commands_queue_url)

aws sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body '{"payoutId":"pay_test_001","correlationId":"corr-123","apm":"WERO","amount":150.00,"currency":"EUR","beneficiaryIban":"DE89370400440532013000"}'
  
aws sqs receive-message \
  --queue-url "$QUEUE_URL" \
  --max-number-of-messages 10 \
  --visibility-timeout 0 \
  --attribute-names All \
  --message-attribute-names All
  
###### FORCE MESSAGE INTO DLQ
  
QUEUE_URL=$(cd ~/IdeaProjects/payment-ecosystem/infra && terraform output -raw payout_commands_queue_url)

# 1. send msg to principal queue
aws sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body '{"payoutId":"pay_force_dlq_001","correlationId":"corr-999","apm":"WERO","amount":150.00,"currency":"EUR","beneficiaryIban":"DE89370400440532013000"}'

# 2. Simulate 3 receipts without delete-message (reaches maxReceiveCount = 3)
for i in 1 2 3; do
  echo "Tentativa de leitura $i..."
  aws sqs receive-message --queue-url "$QUEUE_URL" --max-number-of-messages 1 --visibility-timeout 1 > /dev/null
  sleep 2
done

# 3. message finally goes to DLQ and does not return
aws sqs receive-message --queue-url "$QUEUE_URL" --max-number-of-messages 1 --visibility-timeout 1

##### CHECK MESSAGES IN DLQ

DLQ_URL=$(cd ~/IdeaProjects/payment-ecosystem/infra && terraform output -raw payout_commands_dlq_url)

aws sqs receive-message \
  --queue-url "$DLQ_URL" \
  --max-number-of-messages 1 \
  --visibility-timeout 1 \
  --attribute-names All \
  --message-attribute-names All
  
##### EXECUTE DLQ REDRIVE MANUALLY

DLQ_ARN=$(aws sqs get-queue-attributes --queue-url "$(cd ~/IdeaProjects/payment-ecosystem/infra && terraform output -raw payout_commands_dlq_url)" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
SRC_ARN=$(aws sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

aws sqs start-message-move-task \
  --source-arn "$DLQ_ARN" \
  --destination-arn "$SRC_ARN"
  
##### CHECK PAYOUT-SERVICE + P2A

# Publish a message in SQS
aws sqs send-message \
  --queue-url $(aws sqs get-queue-url --queue-name payment-ecosystem-payout-commands --query 'QueueUrl' --output text) \
  --message-body '{
    "payoutId": "pay_local_001",
    "correlationId": "corr_local_001",
    "apm": "WERO",
    "amount": 250.00,
    "currency": "EUR",
    "beneficiaryIban": "DE89370400440532013000"
  }'
  
# Logs of payout-service (consumption from SQS -> HTTP call to p2a -> insert in Postgres)
docker logs -f payout-service

# Logs do p2a-service (receipt of POST /webhooks/p2a/status-updates -> publish in Kafka)
docker logs -f p2a-service

# check DB
docker exec -it payment-postgres psql -U payout_user -d payout_db

# check table structure
\d payout_requests

# check table content
SELECT
    payout_id,
    status,
    jsonb_pretty(payload) AS payload,
    created_at,
    version
FROM payout_requests
ORDER BY created_at DESC;
```
