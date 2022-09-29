import docker

from ...common.python.database.logger import Logger


# Stub version of run_processing_in_docker to test other python code without
# actually launching the docker (when it takes lots of time for example).
def run_processing_in_docker_stub(
    image_name: str,
    container_name: str,
    command: str,
    volumes_binding: dict,
    logger: Logger
):
    return 0


def run_processing_in_docker(
    image_name: str,
    container_name: str,
    command: str,
    volumes_binding: dict,
    logger: Logger
):
    client = docker.from_env()

    # Before running our container, be sure there is not another one that
    # exists.
    existing_containers = client.containers.list(
        filters={'name': container_name},
        all=True
        )
    if len(existing_containers) == 1:
        # For now, as we are not sure about the condition when this happens, we
        # consider that it is best to remove the container instead of interupt
        # the workflow with an internal error.
        logger.warning(
            f'docker container "{container_name}" already exists, remove it"')
        container_to_remove = existing_containers[0]
        container_to_remove.remove(force=True)

    # Actually run the processing.
    container = client.containers.run(
        image_name,
        name=container_name,
        command=command,
        volumes=volumes_binding,
        # Unconfine docker run to try to prevent OCI errors to appear randomly.
        security_opt=["seccomp=unconfined"],
        # Set the detach to capture stdout below.
        detach=True)

    # Stream the stdout to the logger during the container execution.
    for line in container.logs(stream=True, stdout=True, stderr=False):
        logger.info(line.decode('utf-8').rstrip())

    # At this point, the container has exited. But we use the "wait" method to
    # get the exit code.
    response = container.wait()
    status = response['StatusCode']
    if status != 0:
        # On error, we stream stderr to the logger
        for line in container.logs(stream=True, stdout=False, stderr=True):
            logger.error(line.decode('utf-8').rstrip())

    # We are done with this container, we can remove it.
    container.remove()

    return status