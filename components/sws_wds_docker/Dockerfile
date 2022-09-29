FROM ubuntu:20.04

LABEL authors="Markus Hetzenecker, Lars Keuris"
LABEL maintainer="Markus.Hetzenecker@enveo.at, Lars.Keuris@enveo.at"
LABEL Description="EEA_PanEU_HR_SI_Part2 WetSnowProcessing" Vendor="ENVEO IT" Version="1.0"

ENV TZ="UTC"
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive
    
#basic system installs
RUN apt-get update -y --fix-missing && apt-get -y upgrade
RUN apt-get install -y --fix-missing curl rclone python3 python3-numpy python3-scipy python3-yaml python3-requests gdal-bin python3-gdal python3-dateutil python3-boto3
#RUN apt-get install -y default-jre

# addon for devel
# RUN apt-get install -y openssh-server tightvncserver apt-utils file xterm vim build-essential tar wget
RUN apt-get install -y wget

#python pip packages
# RUN apt-get install -y python3-pip
# RUN pip3 install numpy scipy shapely fiona pyyaml lxml

#create necessary directories
# RUN mkdir /src /install /work /eodata /data

##############################
# create user
RUN groupadd -g 1000 eouser && useradd -m -g 1000 -u 1000 -s /bin/bash eouser

##############################

# install SNAP
WORKDIR /src
COPY response.varfile ./
RUN curl -o snap.sh https://download.esa.int/step/snap/8.0/installers/esa-snap_sentinel_unix_8_0.sh && sh ./snap.sh -q -dir /install/snap  -varfile response.varfile
RUN echo snap.versionCheck.interval=NEVER >> /install/snap/etc/snap.properties
RUN mv /install/snap/bin/gpt.vmoptions /install/snap/bin/gpt.vmoptions.orig
RUN echo /install/snap/platform/lib > /etc/ld.so.conf.d/snap.conf && /sbin/ldconfig
RUN /install/snap/bin/snap --nosplash --nogui --modules --update-all 2>&1 | while read -r line; do echo $line; [ "$line" = "updates=0" ] && sleep 2 && pkill -TERM -f "snap/jre/bin/java"; done; /bin/true
RUN su -l eouser -c "/install/snap/bin/gpt --diag"
##############################

# install Sentinel (Wet) Snow Processing scripts
ADD *.py  /install/ssp/bin/

#delete src dir
WORKDIR /
RUN rm -r /src /root/.snap /root/.java || :

RUN wget http://step.esa.int/auxdata/dem/egm96/ww15mgh_b.zip -O ww15mgh_b.zip; \
    mkdir -p /root/.snap/auxdata/dem/egm96/; \
    mv ww15mgh_b.zip /root/.snap/auxdata/dem/egm96/
