output "ecr_repository_url" {
  description = "URL do repositorio ECR para dar docker push"
  value       = aws_ecr_repository.p2a.repository_url
}

output "msk_bootstrap_brokers_iam" {
  description = "Endpoints IAM do cluster MSK"
  value       = aws_msk_cluster.kafka.bootstrap_brokers_sasl_iam
}

output "webhook_url" {
  description = "URL publica do ALB para disparar o webhook do P2A"
  value       = "http://${aws_lb.p2a_alb.dns_name}/webhooks/p2a/status-updates"
}
