from ....common.python.util.datetime_util import DatetimeUtil

# check if environment variable is set, exit in error if it's not
from ....common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

from ....common.python.sentinel.sentinel2_product import Sentinel2Product
from ....common.python.database.model.job.fsc_rlie_job import FscRlieJob

def test_FscRlieJob_creation():
    """Test that jobs presence in database is correctly verified."""

    # Variables definition
    productIdentifier = "/eodata/Sentinel-2/MSI/L1C/2016/05/28/S2A_"\
                "MSIL1C_20160528T104032_N0202_R008_T31TGL_20160528T104248.SAFE"
    snowCover = 0
    cloudCover = 6.0894
    published = DatetimeUtil.fromRfc3339("2019-10-25T01:59:37.686458Z")

    # Creation of sentinel2 product needed for a new FscRlieJob job
    s2_product = Sentinel2Product(productIdentifier, published, cloudCover, snowCover)

    # Create jobs to store in DataBase
    job = FscRlieJob(tile_id=s2_product.tile_id,
                    l1c_id=s2_product.product_id,
                    l1c_path=s2_product.product_path,
                    l1c_cloud_cover=s2_product.cloud_cover,
                    l1c_snow_cover=s2_product.snow_cover,
                    measurement_date=s2_product.measurement_date,
                    l1c_esa_creation_date=s2_product.esa_creation_date,
                    # There is also the 'update' param that we don't use.
                    l1c_dias_publication_date=s2_product.dias_publication_date)
