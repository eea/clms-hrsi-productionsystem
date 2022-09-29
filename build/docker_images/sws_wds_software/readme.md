This directory contains the scripts to build the Docker images needed for the
SWS WDS software.

## Presentation

There is only one image containing external dependencies and tools to execute
SWS WDS processings.

The image has to be build in an instance in the OpenStack project (like
`tf-toto` for example). The images are then stores in the `foo` bucket for
future use (by workers for the final image, mainly).

The name of the images look like:
* `wetsnowprocessing:git-xxxxxxxx.tar`

Where the tags correspond to the hash of the current git commit when the build
has been launch.


## Building the image

The docker image needs to be rebuild when some content of [components/sws_wds_docker]
(../../../components/sws_wds_docker) changes.

If you are the one performing updates on the software, make sure to commit your 
changes before building the new docker image.

It is **very important** to do this commit, as the docker image will be tagged
with the new commit. This is needed so we can trace the software evolutions.

Once the commit is done, to build the final image execute the following command:

``` bash
$ sudo ./build_sws_wds_image.sh
```

This will build the image locally and upload it in the bucket.
