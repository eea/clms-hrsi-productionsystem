resource "openstack_objectstorage_container_v1" "hrsi_products" {
  name = "HRSI"
  region = "RegionOne"
  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}

resource "openstack_objectstorage_container_v1" "sip_results" {
  name = "hidden_value"
  region = "RegionOne"
  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}

resource "openstack_objectstorage_container_v1" "sip_aux" {
  name = "hidden_value"
  region = "RegionOne"
  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}

resource "openstack_objectstorage_container_v1" "sip_data" {
  name = "hidden_value"
  region = "RegionOne"
  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}

resource "openstack_objectstorage_container_v1" "infra" {
  name = "foo"
  region = "RegionOne"
  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}

resource "openstack_objectstorage_container_v1" "backups" {
  name = "hidden_value"
  region = "RegionOne"
  # We explicitly prevent destruction using terraform. Remove this only if you
  # really know what you're doing.
  lifecycle {
    prevent_destroy = true
  }
}
