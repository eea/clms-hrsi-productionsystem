#!/bin/bash

# Be sure we fail on error and output debugging information
set -e
set -o pipefail
trap 'log "error on line $LINENO" ; set_job_status "$job_id" failed' ERR

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

pytest tests/infra/infra_tests.py
