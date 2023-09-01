job "dp-search-reindex-batch" {
  datacenters = ["eu-west-2"]
  region      = "eu"
  type        = "batch"

  meta {
    run_uuid = "${uuidv4()}"
  }

  group "publishing" {
    count = "{{PUBLISHING_TASK_COUNT}}"

    constraint {
      attribute = "${node.class}"
      value     = "publishing-mount"
    }

    restart {
      attempts = 3
      delay    = "15s"
      interval = "1m"
      mode     = "delay"
    }

    task "dp-search-reindex-batch" {
      driver = "docker"

      artifact {
        source = "s3::https://s3-eu-west-2.amazonaws.com/{{DEPLOYMENT_BUCKET}}/dp-search-reindex-batch/{{PROFILE}}/{{RELEASE}}.tar.gz"
      }

      config {
        command = "${NOMAD_TASK_DIR}/start-task"

        args = ["./dp-search-reindex-batch"]

        image = "{{ECR_URL}}:concourse-{{REVISION}}"

        mounts = [
          {
            type     = "bind"
            target   = "/content"
            source   = "/var/florence"
            readonly = false
          }
        ]
      }

      resources {
        cpu    = "{{PUBLISHING_RESOURCE_CPU}}"
        memory = "{{PUBLISHING_RESOURCE_MEM}}"

        network {
          port "http" {}
        }
      }

      template {
        source      = "${NOMAD_TASK_DIR}/vars-template"
        destination = "${NOMAD_TASK_DIR}/vars"
      }

      vault {
        policies = ["dp-search-reindex-batch"]
      }
    }
  }
}
