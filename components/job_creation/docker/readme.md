
# Temporary instructions

From the project GIT root directory, create the `requirements.txt` files:
```sh
pipreqs --force components/common/python
pipreqs --force components/job_creation/python
```

Build the Docker image:
```sh
docker build --rm -t csi_job_creation -f components/job_creation/docker/Dockerfile .
```

Run the Docker image.<br> 
TODO adapt:
  * COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000
  * --network=host
```sh
docker run --rm \
    --env COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000 \
    --env CSI_SCIHUB_ACCOUNT_PASSWORD=dummy_value \
    --env CSI_SIP_DATA_BUCKET=dummy_value \
    --network=host \
    --name csi_job_creation_c \
    csi_job_creation
```
