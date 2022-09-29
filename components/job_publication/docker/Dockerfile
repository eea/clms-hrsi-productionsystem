
# See:
# https://blog.realkinetic.com/building-minimal-docker-containers-for-python-applications-37d0272c52f3
# https://pythonspeed.com/articles/base-image-python-docker-images/
FROM python:3.7-slim-buster as base

## Arguments

# CoSIMS components directory from the client machine.
# Run the Docker file from the client /cosims root GIT directory.
ARG components_in=./components

# Used internally
ARG pip_csi=common/docker/pip_csi_python.sh
ARG requirements=python/requirements.txt
ARG components_out=./components

# Copy the CoSIMS component requirements.txt files
COPY ${components_in}/${pip_csi} ${components_out}/${pip_csi}
COPY ${components_in}/common/${requirements} ${components_out}/common/${requirements}
COPY ${components_in}/job_publication/${requirements} ${components_out}/job_publication/${requirements}
COPY ${components_in}/job_creation/${requirements} ${components_out}/job_creation/${requirements}

# Pip install the CoSIMS Python component requirements
RUN ${components_out}/${pip_csi}

# Copy the CoSIMS components
COPY ${components_in}/common ${components_out}/common
COPY ${components_in}/job_creation ${components_out}/job_creation
COPY ${components_in}/job_publication ${components_out}/job_publication
COPY ${components_in}/job_creation ${components_out}/job_creation 

# Call the main Python module
CMD ["python3", "-c", "from components.job_publication.python.job_publication import JobPublication; JobPublication.start()"]
    
    
