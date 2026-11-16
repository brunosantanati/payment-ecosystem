variable "aws_region" {
  description = "Regiao da AWS onde os recursos serao criados"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefixo para nomear todos os recursos do ecossistema"
  type        = string
  default     = "payment-ecosystem"
}

variable "p2a_image_tag" {
  description = "Tag da imagem Docker do P2A enviada ao ECR"
  type        = string
  default     = "latest"
}
