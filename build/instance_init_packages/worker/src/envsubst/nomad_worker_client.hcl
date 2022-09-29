datacenter = "dc1"
data_dir = "/opt/nomad"

client {
  enabled = true
  servers = ["${CSI_NOMAD_SERVER_IP}:4647"]
  node_class = "worker"
}

plugin "raw_exec" {
  config {
    enabled = true
  }
}
