
# Temporary instructions

From the project GIT root directory, create the `requirements.txt` files:
```sh
pipreqs --force components/common/python
pipreqs --force components/job_execution/python
```

Build the Docker image:
```sh
docker build --rm -t csi_job_execution -f components/job_execution/docker/Dockerfile .
```

Run the Docker image.<br> 
TODO adapt:
  * COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000
  * --network=host
```sh
docker run --rm \
    --env COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000 \
    --network=host \
    --name csi_job_execution_c \
    csi_job_execution
```