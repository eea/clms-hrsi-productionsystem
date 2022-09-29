FROM alpine

RUN \
  apk add --no-cache \
    bash \
    py-pip \
    jq \
  && pip install yq

RUN mkdir -p /cosims/stub_data
ADD dummy_outputs /cosims/stub_data/outputs

RUN mkdir -p /cosims/bin
COPY cosims_processings_stub.sh /cosims/bin

CMD \
  /cosims/bin/cosims_processings_stub.sh \
  /cosims/stub_data \
  $csi_si_software_parameter_file
