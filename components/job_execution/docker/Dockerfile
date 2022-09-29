
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

# Install Nomad and dependencies
RUN \
  apt update && apt install --yes curl unzip \
  && nomad_version="0.11.1" \
  && curl \
  --silent \
  --remote-name https://releases.hashicorp.com/nomad/"$nomad_version"/nomad_"$nomad_version"_linux_amd64.zip \
  && unzip nomad_"$nomad_version"_linux_amd64.zip \
  && mv nomad /usr/local/bin/ \
  && rm nomad_"$nomad_version"_linux_amd64.zip \
  && apt-get purge -y --auto-remove curl unzip \
  && apt clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy the CoSIMS component requirements.txt files
COPY ${components_in}/${pip_csi} ${components_out}/${pip_csi}
COPY ${components_in}/common/${requirements} ${components_out}/common/${requirements}
COPY ${components_in}/job_execution/${requirements} ${components_out}/job_execution/${requirements}
COPY ${components_in}/job_creation/${requirements} ${components_out}/job_creation/${requirements}

# Pip install the CoSIMS Python component requirements
RUN ${components_out}/${pip_csi}

# Copy the CoSIMS components
COPY ${components_in}/common ${components_out}/common
COPY ${components_in}/job_execution ${components_out}/job_execution
COPY ${components_in}/job_creation ${components_out}/job_creation 

# Call the main Python module
CMD ["python3", "-c", "from components.job_execution.python.job_execution import JobExecution ; JobExecution.start()"]
