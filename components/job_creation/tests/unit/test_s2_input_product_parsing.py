from datetime import datetime

from ....common.python.util.creodias_util import CreodiasUtil
from ....common.python.sentinel.sentinel2_l1c_id import Sentinel2L1cId

# check if environment variable is set, exit in error if it's not
from ....common.python.util.sys_util import SysUtil
SysUtil.ensure_env_var_set("COSIMS_DB_HTTP_API_BASE_URL")

def test_s2_parse_product_id():
    """Test that L1C features are correctly parsed."""

    # Constants definition
    datetime_strptime = '%Y%m%dT%H%M%S'
    ref_sentinel2_id = "S2A_MSIL1C_20160528T104032_N0202_R008_T31TGL_20160528T104248.SAFE"

    # Call the function to test
    sentinel_product_id = Sentinel2L1cId(ref_sentinel2_id)

    assert sentinel_product_id.mission == "S2A"
    assert sentinel_product_id.instrument == "MSI"
    assert sentinel_product_id.level == "L1C"
    assert sentinel_product_id.start_time == datetime.strptime("20160528T104032", datetime_strptime)
    assert sentinel_product_id.orbit == "008"
    assert sentinel_product_id.tile == "31TGL"
    assert sentinel_product_id.discriminator == datetime.strptime("20160528T104248", datetime_strptime)


def test_read_input_product_feature():
    """Test that L1C features are correctly parsed."""

    # Constants definition
    json_root = {}
    feature = {
        'properties' : {
            'productIdentifier': "/eodata/Sentinel-2/MSI/L1C/2016/05/28/S2A_"\
                "MSIL1C_20160528T104032_N0202_R008_T31TGL_20160528T104248.SAFE",
            'snowCover': 0,
            'cloudCover': 6.0894,
            'platform': "S2A",
            'productType': "L1C",
            'instrument': "MSI",
            'published': "2019-10-25T01:59:37.686458Z"
        }
    }
    feature_index = 19

    # Call the function to test
    sentinel_product = CreodiasUtil.read_input_product_feature(
        json_root,
        feature,
        feature_index
    )

    # Values to be compared
    computed_values = [
        sentinel_product.tile_id,
        sentinel_product.product_id,
        sentinel_product.product_path,
        sentinel_product.cloud_cover,
        sentinel_product.snow_cover,
        sentinel_product.measurement_date,
        sentinel_product.dias_publication_date.replace(tzinfo=None)
    ]
    reference_values = [
        '31TGL',
        'S2A_MSIL1C_20160528T104032_N0202_R008_T31TGL_20160528T104248',
        '/eodata/Sentinel-2/MSI/L1C/2016/05/28/S2A_MSIL1C_20160528T104032'\
            '_N0202_R008_T31TGL_20160528T104248.SAFE',
        6.0894,
        0,
        datetime.strptime('20160528T104032', '%Y%m%dT%H%M%S'),
        datetime.strptime('2019-10-25T01:59:37.686458', '%Y-%m-%dT%H:%M:%S.%f')
    ]

    # Ensure that the computed values are the same than the reference ones
    tested_params = 7
    for i in range(tested_params):
        assert computed_values[i] == reference_values[i], \
            "Error : computed_values[%s] not matching reference_values[%s], %s != %s" %(
                i,
                i,
                computed_values[i],
                reference_values[i]
            )


def test_read_json_param():
    """Test that JSON parameters are correctly parsed."""

    # Constants definition
    json_root = {}
    feature = {
        'properties' : {
            'productIdentifier': "/eodata/Sentinel-2/MSI/L1C/2016/05/28/S2A_"\
                "MSIL1C_20160528T104032_N0202_R008_T31TGL_20160528T104248.SAFE",
            'snowCover': 0,
            'cloudCover': 6.0894,
            'platform': "S2A",
            'productType': "L1C",
            'instrument': "MSI",
            'published': "2019-10-25T01:59:37.686458Z"
        }
    }
    feature_index = 19
    json_param = "published"
    param_reference_value = feature['properties']['published']

    # Call the function to test
    param_value = CreodiasUtil.read_json_param(
        json_root,
        feature,
        feature_index,
        json_param
    )

    # Ensure that the parsed parameter value is correct
    assert param_value == param_reference_value, \
        "Error : json parameters parsed incorrectly, %s != %s" %(
            param_value,
            param_reference_value
            )
