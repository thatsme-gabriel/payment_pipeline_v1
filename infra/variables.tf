variable "database" {
  description = "Coordenadas nao secretas do banco Neon direto"
  type        = map(string)
}

variable "cdc_password" {
  type      = string
  sensitive = true
}
