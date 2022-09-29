
# See:
# https://blog.realkinetic.com/building-minimal-docker-containers-for-python-applications-37d0272c52f3
# https://pythonspeed.com/articles/base-image-python-docker-images/
FROM python:3.7-slim-buster as base

## Arguments

# basic system installs for sws_wds_job
RUN apt-get update && apt-get install -y --fix-missing gdal-bin python3-gdal python3-distutils
ENV PYTHONPATH="${PYTHONPATH}:/home/eouser/.local/lib/python3.7/site-packages:/usr/local/lib/python3.7/dist-packages:/usr/lib/python3/dist-packages:/usr/local/lib/python3.7/:/usr/local/lib/python3.7/site-packages:/usr/local/lib/python3.7/lib-dynload:/usr/local/lib/python37.zip"

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
COPY ${components_in}/job_creation/${requirements} ${components_out}/job_creation/${requirements}

# Pip install the CoSIMS Python component requirements
RUN ${components_out}/${pip_csi}

# Copy the CoSIMS components
COPY ${components_in}/common ${components_out}/common
COPY ${components_in}/job_creation ${components_out}/job_creation

# Call the main Python module
CMD ["python3", "-c", "from components.job_creation.python.job_creation import JobCreation; JobCreation.start()"] 
