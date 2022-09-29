from builtins import staticmethod
from os.path import realpath, dirname


class ResourceUtil(object):
    '''Utility functions to access the project resources.'''

    # Project root folder, calculated from this module path
    ROOT_FOLDER = realpath(dirname(realpath(__file__)) + "/../../../..")

    # /root/components folder
    COMPONENTS_FOLDER = ROOT_FOLDER + '/components/'

    @staticmethod
    def for_component(relative_path):
        '''Return the path of a resource under the /components folder.'''
        return ResourceUtil.COMPONENTS_FOLDER + relative_path
