
# Temporary instructions

From the project GIT root directory, create the `requirements.txt` files:
```sh
pipreqs --force components/common/python
```

Build the Docker image:
```sh
docker build --rm -t csi_common -f components/common/docker/Dockerfile .
```