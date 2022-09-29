This docker compose file is used to build and push docker images for the
project.

Inputs are managed with environment variables (with fallback to default
values if these variables are not set):
- CSI_DOCKER_REGISTRY_IMAGE (default to csi): the image prefix. For CI/CD this
  contains the registry where to push the images (GitLab one for example).
- CSI_DOCKER_TAG (default to local): the tag to apply. Usually the git hash
  when used from a CI/CD.

## Example

On a local machine simply use:

``` shell
$ docker-compose build
```

This will build docker images like `csi/job_execution:local`.


If you also want to push on a registry and use a specifi tag, use:

``` shell
export CSI_DOCKER_REGISTRY_IMAGE="registry-ext.magellium.com:443/cosims/cosims"
export CSI_DOCKER_TAG="my_tag"
docker-compose build
docker-compose push
```

This will build and push images like `registry-ext.magellium.com:443/cosims/cosims/job_creation:my_tag`.

# Why docker compose?

We choose to use docker compose to build and push based on the following article
"[Building Docker Images using Docker Compose and Gitlab CI/CD](https://vgarcia.dev/blog/2019-06-17-building-docker-images-using-docker-compose-and-gitlab/)".
This article "outlines how to use Docker, Docker Compose, GitLab CI/CD and the
GitLab Container Registry to build, tag and push docker images. [...]
Docker-compose provides a higher level of abstraction than the Docker-CLI. It
not only allows us to consume already-built images, but it's also a great tool
for defining how to build new images in a clean and declarative way. "