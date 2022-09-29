FROM debian:buster-slim as builder

WORKDIR /tmp/
RUN \
  apt-get update \
  && apt-get install --yes curl unzip \
  && nomad_version="0.11.1" \
  && curl \
      --silent \
      --remote-name https://releases.hashicorp.com/nomad/"$nomad_version"/nomad_"$nomad_version"_linux_amd64.zip \
  && unzip nomad_"$nomad_version"_linux_amd64.zip \
  && terraform_version="0.12.24" \
  && curl \
      --silent \
      --remote-name https://releases.hashicorp.com/terraform/"$terraform_version"/terraform_"$terraform_version"_linux_amd64.zip \
  && unzip terraform_"$terraform_version"_linux_amd64.zip


FROM python:3.7-slim-buster


RUN \
  apt-get update \
  && apt-get install --yes \
    # Need to install 'ca-certificates' for terraform to work. Otherwise terraform
    # init with an OpenStack provider exit with an error like:
    #   Error: Post https://cf2.cloudferro.com:5000/v3/auth/tokens: OpenStack connection error, retries exhausted. Aborting. Last error was: x509: certificate signed by unknown authority
    ca-certificates \
    # Tools needed by some terraform "data.external" resources
    jq \
    python3-openstackclient \
    curl

COPY --from=builder /tmp/nomad /usr/local/bin/
COPY --from=builder /tmp/terraform /usr/local/bin/

RUN mkdir /opt/csi

COPY ./common /opt/csi/common
COPY ./config /opt/csi/config

ARG components_in=/components

# Used internally
ARG pip_csi=common/docker/pip_csi_python.sh
ARG requirements=python/requirements.txt
ARG components_out=/opt/csi/components

# Copy the CoSIMS component requirements.txt files
COPY ${components_in}/${pip_csi} ${components_out}/${pip_csi}
COPY ${components_in}/common/${requirements} ${components_out}/common/${requirements}
COPY ${components_in}/worker_pool/${requirements} ${components_out}/worker_pool/${requirements}
COPY ${components_in}/job_creation/${requirements} ${components_out}/job_creation/${requirements}

RUN \
  apt-get install --yes --no-install-recommends gcc python3-dev

# Pip install the CoSIMS Python component requirements
RUN ${components_out}/${pip_csi}

RUN apt-get purge --yes --auto-remove gcc python3-dev

# Copy the CoSIMS components
COPY ${components_in}/common ${components_out}/common
COPY ${components_in}/worker_pool ${components_out}/worker_pool
COPY ${components_in}/job_creation ${components_out}/job_creation 

WORKDIR /opt/csi
# Call the main Python module
CMD ["python3", "-c", "from components.worker_pool.python.worker_pool_management import WorkerPoolManagement; WorkerPoolManagement.start()"] 
