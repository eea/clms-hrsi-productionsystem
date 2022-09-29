# check if environment variable is set, exit in error if it's not
from ...python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

from ...python.database.model.job.job_types import JobTypes
from ...python.util.log_util import temp_logger


def test_job_types():
    """Test the job type list"""

    # Get the job list
    job_list = JobTypes.get_job_type_list(temp_logger)

    # Ensure that the job type list is not empty
    assert len(job_list)!=0, "Error : job list is empty"
