resource "aws_default_vpc" "default" {
  tags = {
    Name = "Default VPC"
  }
}

resource "aws_security_group" "postgres" {
  vpc_id = aws_default_vpc.default.id
  name = "postgres"

  ingress {
    protocol  = "tcp"
    self      = true
    from_port = 5432
    to_port   = 5432
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "postgres"
  }
}

resource "aws_db_instance" "covid19" {
  identifier          = "etlcovid19"
  name                = "${var.name}"
  username            = "${var.user}"
  password            = "${random_string.password.result}"
  publicly_accessible = true
  engine              = "postgres"
  engine_version      = "12.4"
  instance_class      = "db.t2.micro"
  skip_final_snapshot = true
  apply_immediately   = true
  vpc_security_group_ids = [aws_security_group.postgres.id]
  allocated_storage   = 10
  max_allocated_storage = 100
}
