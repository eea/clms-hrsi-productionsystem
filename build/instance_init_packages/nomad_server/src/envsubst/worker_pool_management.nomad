job "worker-pool-management" {
  type        = "service"
  datacenters = ["dc1"]

  constraint {
    attribute = "${node.class}"
    value = "orchestrator"
  }

  task "worker-pool-management" {
    driver = "docker"

    config {
      image = "registry-ext.magellium.com:443/cosims/cosims/worker_pool:git-xxxxxxxx"
      auth {
        username = "gitlab+deploy-token-4"
        password = "$GITLAB_TOKEN_PASSWORD"
      }
    }

    resources {
      cpu = 1000
      memory = 1000
    }

    logs {
      max_files = 29
      max_file_size = 10
    }

    template {
      env = true
      destination = "csi_config.env"
      data = <<EOF
$CSI_CONFIG_ENV_FILE_CONTENT
EOF
    }

    template {
      env = true
      destination = "${NOMAD_SECRETS_DIR}/csi_secrets.env"
      data = <<EOF
$CSI_SECRETS_ENV_FILE_CONTENT
EOF
    }
  }
}
