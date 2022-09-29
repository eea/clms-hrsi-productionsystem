
# Temporary instructions

From the project GIT root directory, create the `requirements.txt` files:
```sh
pipreqs --force components/common/python
pipreqs --force components/monitor/python
```

Build the Docker image:
```sh
docker build --rm -t csi_monitor -f components/monitor/docker/Dockerfile .
```

Run the Docker image.<br> 
TODO adapt:
  * COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000
  * --network=host
```sh
docker run --rm \
    --env COSIMS_DB_HTTP_API_BASE_URL=http://localhost:3000 \
    --env CSI_NOMAD_SERVER_IP=localhost \
    --env CSI_HTTP_API_INSTANCE_IP=localhost \
    --env CSI_SCIHUB_ACCOUNT_PASSWORD=dummy_value \
    --env CSI_SIP_DATA_BUCKET=dummy_value \
    --network=host \
    --name csi_monitor_c \
    csi_monitor
```
