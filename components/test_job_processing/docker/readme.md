
# Temporary instructions


Build the Docker image:
```sh
docker build --rm -t csi_test_job_processing -f components/test_job_processing/docker/Dockerfile .
```

Run the Docker image.<br> 
TODO adapt:
  * COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000
  * --network=host
```sh
docker run --rm \
    --env COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000 \
    --network=host \
    --name csi_test_job_processing_c \
    csi_test_job_processing \
    test_job_processing_chain.py
```
