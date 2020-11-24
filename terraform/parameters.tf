resource "aws_ssm_parameter" "endpoint" {
  name        = "/database/${var.name}/endpoint"
  description = "Endpoint to connect to the ${var.name} database"
  type        = "SecureString"
  value       = "${aws_db_instance.covid19.address}"
}

resource "aws_ssm_parameter" "user" {
  name        = "/database/${var.name}/user"
  description = "Name of the ${var.name} database"
  type        = "SecureString"
  value       = "${var.user}"
}

resource "aws_ssm_parameter" "password" {
  name        = "/database/${var.name}/password"
  description = "Password to the ${var.name} database"
  type        = "SecureString"
  value       = "${random_string.password.result}"
}

resource "aws_ssm_parameter" "database" {
  name        = "/database/${var.name}/database"
  description = "Name of the ${var.name} database"
  type        = "SecureString"
  value       = "${var.name}"
}