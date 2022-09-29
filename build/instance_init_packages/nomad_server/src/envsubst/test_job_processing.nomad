job "test-job-processing" {
  type        = "batch"
  datacenters = ["dc1"]

  constraint {
    attribute = "${node.class}"
    value = "worker"
  }

  constraint {
    attribute = "${attr.platform.aws.instance-type}"
    value = "eo1.xsmall"
  }

  parameterized {
    meta_required = ["job_id"]
    meta_required = ["job_unique_id"]
  }

  // This group launches the task that actually runs the worker (and then the
  // S&I processings, with donwload/upload of data, etc.) and another task that
  // uploads in a bucket the Nomad allocation directory every now and then and
  // at the end of the worker.
  //
  // For that the first task (worker) is set as the leader task which tells
  // Nomad to stop the second one when it is finished.
  group "run-and-log" {

    // To execute every thing that is needed to prepare S&I processings, run it
    // and manage what it produces.
    task "run-worker" {
      // So that the second task will be stopped when this one is finished.
      leader = true

      driver = "raw_exec"

      config {
        command = "$CSI_ROOT_DIR/worker/components/worker/src/run_test_job.sh"
        args = [ "${NOMAD_META_JOB_ID}" ]
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

      resources {
        cpu = 50
        memory = 500
      }
    }

    // Some kind of sidecar task to upload the Nomad allocation directory in a
    // bucket for use when the Nomad job files are clean up (during Nomad
    // garbage collection or when the worker is destroyed).
    task "upload-nomad-alloc-dir" {
      driver = "raw_exec"

      // Set a high enough timeout to be sure that the last upload has time to
      // finish.
      kill_timeout = "100s"

      // For now, just put this lenghty bash script in a
      // "bash -s '<the script>'" like command. Which is enough and has the
      // advantage to be explicit. Maybe it will be better some time to put this
      // in an external script file.
      config {
        command = "bash"
        args = [ "-c", <<EOF
          echo start uploading...

          if [ ! -v CSI_BUCKET_NAME_SIP_RESULTS ]; then
            echo the required environment variable 'CSI_BUCKET_NAME_SIP_RESULTS' is missing
            exit 1
          fi

          upload_alloc_dir () {
            # Make a copy of what we need to upload to freeze the content. If
            # not, the logs can be updated while rclone is uploading them,
            # leading to a mismatch content between the bucket and the local
            # file at the end of rclone upload. At this time rclone does a
            # checksum of the content which raises an error.
            rm -rf directory_to_upload
            cp -Rp ${NOMAD_ALLOC_DIR} directory_to_upload

            # Nomad uses some FIFO files (which are named *.fifo) which we don't
            # want to upload. Delete them first. FIFO files are also known as
            # named pipe, which type is p for the find command.
            find directory_to_upload -type p -delete

            # Actually upload
            rclone sync \
              directory_to_upload \
              foo$CSI_BUCKET_NAME_SIP_RESULTS/work/jobs/${NOMAD_META_JOB_UNIQUE_ID}/nomad/allocs/${NOMAD_ALLOC_ID}

            # Add the current alloc ID to the allocs list in the bucket if it is
            # not already present in the list.
            allocs_list_file=allocs_list.txt
            allocs_list_url=foo$CSI_BUCKET_NAME_SIP_RESULTS/work/jobs/${NOMAD_META_JOB_UNIQUE_ID}/nomad/allocs/$allocs_list_file
            rclone copy $allocs_list_url .
            # Be sure the local file exists even if it was missing in the bucket
            touch $allocs_list_file
            # The grep check if it is already present, if grep fails the command
            # after the "||" is executed which actually appends the alloc ID to
            # the file.
            cat $allocs_list_file \
              | grep --quiet --line-regexp --fixed-strings "${NOMAD_ALLOC_ID}" \
              || echo "${NOMAD_ALLOC_ID}" >> $allocs_list_file
            # Upload the file to the bucket
            rclone copy $allocs_list_file foo$CSI_BUCKET_NAME_SIP_RESULTS/work/jobs/${NOMAD_META_JOB_UNIQUE_ID}/nomad/allocs
          }

          # When the leader task is finished, Nomad send a SIGINT signal to this
          # task to ask it to cleanly stop. So we trap this signal and do an
          # ultimate upload of the allocation directory with the last content.
          trap 'upload_alloc_dir ; echo end ; exit' SIGINT

          # Upload the Nomad allocation directory every now and then.
          while true; do
            upload_alloc_dir
            sleep 1
          done
EOF
       ]
      }

      template {
        env = true
        destination = "csi_config.env"
        data = <<EOF
$CSI_CONFIG_ENV_FILE_CONTENT
EOF
      }
   }
  }
}