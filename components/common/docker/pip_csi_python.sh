
#!/bin/bash

# Pip install the CoSIMS Python component requirements

set -o xtrace # Print commands
set -e # Exit on error

if [ -z ${components_out} ]; then
    >&2 echo "'components_out' variable must be set."
    exit 1
fi

# TODO figer la version de pip
# Upgrade pip
# pip install --upgrade pip

# For each CoSIMS Python component, e.g. component=common, or component=job_creation, ...
for component in ${components_out}/*; do

    if [ -f "${component}/python/requirements.txt" ]
    then
        # Install the Python requirements.
        # The requirements.txt file can be created with the pipreqs utility.
        pip install -r ${component}/python/requirements.txt
    fi

done
