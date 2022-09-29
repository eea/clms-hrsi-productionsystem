# See:
# https://blog.realkinetic.com/building-minimal-docker-containers-for-python-applications-37d0272c52f3
# https://pythonspeed.com/articles/base-image-python-docker-images/
FROM python:3.7-slim-buster as base

#create necessary directories
RUN mkdir install install/cosims_python install/cosims_python/cosims install/cosims_python/cosims/components work

## Arguments

# CoSIMS components directory from the client machine.
# Run the Docker file from the client /cosims root GIT directory.
ARG components_in=./components

# Used internally
ARG components_out=/install/cosims_python/cosims/components

# Copy the CoSIMS components
COPY ${components_in}/test_job_processing ${components_out}/test_job_processing


# Add the CoSIMS Python components to the PYTHONPATH env var and programs to the PATH env var
ENV PATH=${components_out}/test_job_processing/python:$PATH
##############################


#go to empty work directory
WORKDIR /work



