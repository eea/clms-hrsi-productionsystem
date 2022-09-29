from enum import Enum


class WorkerFlavors(Enum):
    '''
    Dynamically created workers flavors. Possible values are:
    * EXTRA_SMALL, corresponds to the openstack "eo1.xsmall" flavor,
        RAM = 1GB, Disk = 8GB, CPUs = 1.
    * SMALL, corresponds to the openstack "eo2.medium" flavor,
        RAM = 4GB, Disk = 16GB, CPUs = 1.
    * MEDIUM, corresponds to the openstack "hm.medium" flavor,
        RAM = 16GB, Disk = 64GB, CPUs = 2.
    * LARGE, corresponds to the openstack "hm.large" flavor,
        RAM = 32GB, Disk = 128GB, CPUs = 4.
    * EXTRA_LARGE, corresponds to the openstack "hm.large" flavor,
        RAM = 65GB, Disk = 256GB, CPUs = 8.
    '''

    extra_small = 'eo1.xsmall'
    small = 'eo2.medium'
    medium = 'hm.medium'
    large = 'hm.large'
    extra_large = 'hm.xlarge'
