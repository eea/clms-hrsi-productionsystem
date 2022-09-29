#!/bin/sh

set -e

docker build -t cosims-postgres .
docker-compose up

# DB and PostgREST are running. Test it with:
#   $ curl http://localhost:3000/parent_jobs
#   [
#       {
#           "id": 1,
#           "product_id": "S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443",
#           "job_status": "pending"
#       }, 
#       {
#           "id": 2,
#           "product_id": "S2A_MSIL1C_20171010T003621_N0205_R002_T01WCV_20171010T003615",
#           "job_status": "pending"
#       }
#   ]
