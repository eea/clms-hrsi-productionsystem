
# Temporary instructions

From the project GIT root directory, create the `requirements.txt` files:
```sh
pipreqs --force components/common/python
pipreqs --force components/job_publication/python
```

Build the Docker image:
```sh
docker build --rm -t csi_job_publication -f components/job_publication/docker/Dockerfile .
```

Run the Docker image.<br> 
TODO adapt:
  * COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000
  * --network=host
```sh
docker run --rm \
    --env COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000 \
    --env CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_ACCESS_KEY=0 \
    --env CSI_PRODUCTS_BUCKET_EC2_CREDENTIALS_SECRET_KEY=1 \
    --env CSI_PRODUCT_PUBLICATION_ENDPOINT_ID=dummy_value \
    --env CSI_PRODUCT_PUBLICATION_ENDPOINT_VIRTUAL_HOST=dummy_value \
    --env CSI_PRODUCT_PUBLICATION_ENDPOINT_PASSWORD=dummy_value \
    --env CSI_SCIHUB_ACCOUNT_PASSWORD=dummy_value \
    --env CSI_SIP_DATA_BUCKET=dummy_value \
    --network=host \
    --name csi_job_publication_c \
    csi_job_publication
```
