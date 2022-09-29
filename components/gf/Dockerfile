FROM ubuntu:18.04

MAINTAINER Cemal Melih Tanis "cemal.melih.tanis@fmi.fi"

RUN apt-get update
RUN apt-get install -y wget && rm -rf /var/lib/apt/lists/*
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN sh Miniconda3-latest-Linux-x86_64.sh -b -p ./miniconda

ADD env.yml /
RUN ./miniconda/bin/conda env create -f env.yml

#install programs from source
ADD gf*.py /
ADD *.xml /
ADD README.md /
ADD validate_cloud_optimized_geotiff.py /

ENTRYPOINT ["./miniconda/bin/conda", "run", "--no-capture-output", "-n", "eea_gf"]
