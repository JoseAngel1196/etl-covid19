variable "name" {
    default = "covid19"
}

variable "user" {
    default = "postgres"
}

resource "random_string" "password" {
    length = 16
    special = false
}