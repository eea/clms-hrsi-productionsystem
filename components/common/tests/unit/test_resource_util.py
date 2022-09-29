import os
from os.path import realpath, dirname

# check if environment variable is set, exit in error if it's not
from ...python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

from ...python.util.resource_util import ResourceUtil


def test_for_component():
    """Test that path of a resource under /components folder are correctly built"""

    # Constant definition
    reference_path = realpath(__file__)

    # Call the function to test
    path = ResourceUtil.for_component(
        os.path.join("common", "tests", "unit", "test_resource_util.py"))

    # Ensure path to this test file is correctly built
    assert path == reference_path, "Error : incorrect built path!"