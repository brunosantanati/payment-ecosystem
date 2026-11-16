terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ------------------------------------------------------------------------------
# 1. REDE (Default VPC e Subnets para evitar custo de NAT Gateway)
# ------------------------------------------------------------------------------
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Security Group do ALB (recebe requisicoes HTTP externas no webhook)
resource "aws_security_group" "alb_sg" {
  name        = "${var.project_name}-alb-sg"
  description = "Libera HTTP porta 80 para o webhook"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Security Group da Task ECS (P2A)
resource "aws_security_group" "ecs_sg" {
  name        = "${var.project_name}-ecs-sg"
  description = "Libera trafego do ALB para o container P2A na porta 8080"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.alb_sg.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Security Group do Amazon MSK (aceita conexoes IAM na porta 9098 vindas do ECS)
resource "aws_security_group" "msk_sg" {
  name        = "${var.project_name}-msk-sg"
  description = "Libera trafego Kafka IAM (9098) vindo das tasks ECS"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_sg.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ------------------------------------------------------------------------------
# 2. AMAZON ECR (Repositorio Docker para o P2A)
# ------------------------------------------------------------------------------
resource "aws_ecr_repository" "p2a" {
  name                 = "${var.project_name}-p2a"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

# ------------------------------------------------------------------------------
# 3. AMAZON MSK (Cluster Kafka Provisioned minimo com autenticacao IAM)
# ------------------------------------------------------------------------------
resource "aws_msk_cluster" "kafka" {
  cluster_name           = "${var.project_name}-msk"
  kafka_version          = "3.8.x"
  number_of_broker_nodes = 2 # Minimo exigido pelo MSK (1 broker por AZ)

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = slice(data.aws_subnets.default.ids, 0, 2)
    security_groups = [aws_security_group.msk_sg.id]

    storage_info {
      ebs_storage_info {
        volume_size = 10 # 10 GB por broker
      }
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
}

# ------------------------------------------------------------------------------
# 4. IAM ROLES & PERMISSOES (Privilegio minimo para ECR, CloudWatch e MSK)
# ------------------------------------------------------------------------------
data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Role usada pelo agente do ECS para puxar imagem do ECR e gravar logs no CloudWatch
resource "aws_iam_role" "ecs_task_execution_role" {
  name               = "${var.project_name}-ecs-exec-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ecs_exec_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Role assumida pelo codigo Java do P2A (aws-msk-iam-auth)
resource "aws_iam_role" "p2a_task_role" {
  name               = "${var.project_name}-p2a-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

data "aws_iam_policy_document" "p2a_msk_permissions" {
  statement {
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeClusterDynamicConfiguration"
    ]
    resources = [aws_msk_cluster.kafka.arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:AlterTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:WriteDataIdempotently"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "p2a_msk_inline" {
  name   = "${var.project_name}-p2a-msk-policy"
  role   = aws_iam_role.p2a_task_role.id
  policy = data.aws_iam_policy_document.p2a_msk_permissions.json
}

# ------------------------------------------------------------------------------
# 5. CLOUDWATCH LOGS, ALB & ECS FARGATE
# ------------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "p2a_logs" {
  name              = "/ecs/${var.project_name}/p2a"
  retention_in_days = 1
}

resource "aws_lb" "p2a_alb" {
  name               = "${var.project_name}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb_sg.id]
  subnets            = slice(data.aws_subnets.default.ids, 0, 2)
}

resource "aws_lb_target_group" "p2a_tg" {
  name        = "${var.project_name}-p2a-tg"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = data.aws_vpc.default.id
  target_type = "ip"

  health_check {
    path                = "/"
    matcher             = "200-404" # Aceita 404 na raiz enquanto nao houver Actuator
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.p2a_alb.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.p2a_tg.arn
  }
}

resource "aws_ecs_cluster" "main" {
  name = "${var.project_name}-cluster"
}

resource "aws_ecs_task_definition" "p2a" {
  family                   = "${var.project_name}-p2a-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"  # 0.5 vCPU
  memory                   = "1024" # 1 GB RAM
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.p2a_task_role.arn

  container_definitions = jsonencode([
    {
      name      = "p2a"
      image     = "${aws_ecr_repository.p2a.repository_url}:${var.p2a_image_tag}"
      essential = true

      portMappings = [
        {
          containerPort = 8080
          protocol      = "tcp"
        }
      ]

      environment = [
        {
          name  = "SPRING_KAFKA_BOOTSTRAP_SERVERS"
          value = aws_msk_cluster.kafka.bootstrap_brokers_sasl_iam
        },
        {
          name  = "SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL"
          value = "SASL_SSL"
        },
        {
          name  = "SPRING_KAFKA_PROPERTIES_SASL_MECHANISM"
          value = "AWS_MSK_IAM"
        },
        {
          name  = "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG"
          value = "software.amazon.msk.auth.iam.IAMLoginModule required;"
        },
        {
          name  = "SPRING_KAFKA_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS"
          value = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.p2a_logs.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "p2a" {
  name            = "${var.project_name}-p2a-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.p2a.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = slice(data.aws_subnets.default.ids, 0, 2)
    security_groups  = [aws_security_group.ecs_sg.id]
    assign_public_ip = true # Necessario na Default VPC para puxar imagem do ECR sem NAT Gateway
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.p2a_tg.arn
    container_name   = "p2a"
    container_port   = 8080
  }

  depends_on = [aws_lb_listener.http]
}
