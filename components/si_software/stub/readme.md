This directory contains files to build a docker image for a stub of the docker
image for S&I processing softwares. It behaves like the docker image for S&I
software but without actually executing the S&I processing softwares. It only
copies output files in the expected folder.

This image can be used as is anywhere the official docker image is used. It is
used during the developpment phase of the CoSIMS system when one wants to test
the global behaviour without waiting the very long duration it takes to actually
run S&I processings softwatres.

To build:

``` shell
$ docker build -t cosims_processings_stub .
```

Then use it instead of official docker image.