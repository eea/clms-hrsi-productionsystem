This directory contains the scripts to build the Docker images needed for the
S&I software part-1 and part-2.

## Presentation

There are two images:
* The S&I software part-1, used to generate **FSC/RLIE** products, and itself 
built from two docker images : 
  * The base image, that mainly contains the external dependencies (OTB, SNAP,
    Maja, etc.). This image takes a long time to build (in the order of tens of
    minutes) but doesn't change very often.
  * The final image, that starts with the base image and adds our tools to execute
    S&I processings. This build is a lot faster (in the order of minutes) and its
    content changes more often than for the base image.
* The S&I software part-2, used to generate **RLIE-S1/RLIE-S1+S2** products, 
only built from a single docker image.

The images have to be build on an instance in the OpenStack project (like
`tf-toto` for example). The images are then stores in the `foo` bucket for
future use (by workers for the final image, mainly), under `docker/images/` path.

The names of the images look like:
* `si_software_base:git-xxxxxxxx.tar`
* `si_software:git-5349b886.tar`
* `si_software_part2:git-494a6c94.tar`

Where the tags correspond to the hash of the current git commit when the build
has been launch.


## Building the S&I software part-1 base image

The base image only needs to be rebuilt when the version of the S&I bundle
changes. I.e when there is a modification of the following line in the script
`build_base_image.sh`:

``` bash
bundle_name=docker_install_bundle_6
```

Before launching the build, one can edit the `make` command in file [Dockerfile_protoimage_otb_snap](../../../components/si_software/docker/Dockerfile_protoimage_otb_snap) to augment the number of parrallel build
commands during build of OTB. For example, if running the build from an instance
which flavor is `hm.2xlarge` (in CloudFerro cloud), which has 16 vCPU, one can
replace the build command by `make -j14` to use 14 parallel processes.

To build the base image execute the following command:

``` bash
$ sudo ./build_base_image.sh 
```

This will build the image locally and upload it in the bucket.


## Building the S&I software part-1 final image

The final image needs to be rebuilt when the base image changes or when some
content of [components/si_software](../../../components/si_software) changes.

If the base image has changed, be sure to modify its reference before actually
launching the build of the final image. To do this, modify the script
[build_final_image.sh](../../../build/docker_images/si_software/build_final_image.sh)
and update the following line to point to the new tag for base image:

```
base_image_tag=git-xxxxxxxx
```

Then commit this change.

It is **very important** to do this commit, as the final image will be tagged
with the new commit. This is needed so we can know what base image has been used
with a given final image.

Once the commit is done, to build the final image execute the following command:

``` bash
$ sudo ./build_final_image.sh
```

This will build the image locally and upload it in the bucket.


## Building the S&I software part2 image

The part-2 image needs to be rebuilt when some content of 
[components/si_software/python/si_software_part2]
(../../../components/si_software/python/si_software_part2) changes.
Make sure to commit your changes before building the docker image.

It is **very important** to do this commit, as the docker image will be tagged
with the new commit. This is needed so we can trace the software evolutions.

Once the commit is done, to build the final image execute the following command:

``` bash
$ sudo ./build_part2_image.sh
```

This will build the image locally and upload it in the bucket.

