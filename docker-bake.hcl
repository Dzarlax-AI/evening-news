variable "REVISION" {
  default = "unknown"
}

variable "PLATFORM" {
  default = "linux/arm64"
}

group "default" {
  targets = ["web", "background", "executor"]
}

target "_core" {
  context    = "."
  dockerfile = "docker/Dockerfile"
  platforms  = [PLATFORM]
  args = {
    REVISION = REVISION
  }
  output = ["type=docker"]
}

target "web" {
  inherits = ["_core"]
  target   = "web"
  tags     = ["evening-news-web:consolidation-fixture"]
}

target "background" {
  inherits = ["_core"]
  target   = "background"
  tags     = ["evening-news-background:consolidation-fixture"]
}

target "executor" {
  inherits = ["_core"]
  target   = "executor"
  tags     = ["evening-news-executor:consolidation-fixture"]
}
