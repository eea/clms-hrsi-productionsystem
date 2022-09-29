from datetime import datetime

from ....common.python.sentinel.sentinel1_id import Sentinel1Id

def test_s1_parse_product_id():
    """Test that L1C features are correctly parsed."""

    # Constants definition
    datetime_strptime = '%Y%m%dT%H%M%S'
    ref_sentinel1_id = "S1A_IW_GRDH_1SDV_20210412T151111_20210412T151136_037419_046917_218A.SAFE"

    # Call the function to test
    sentinel_product_id = Sentinel1Id(ref_sentinel1_id)

    assert sentinel_product_id.mission == "S1A"
    assert sentinel_product_id.mode == "IW"
    assert sentinel_product_id.product_type == "GRD"
    assert sentinel_product_id.resolution_class == "H"
    assert sentinel_product_id.processing_level == "1"
    assert sentinel_product_id.product_class == "S"
    assert sentinel_product_id.polarisation == "DV"
    assert sentinel_product_id.start_time == datetime.strptime("20210412T151111", datetime_strptime)
    assert sentinel_product_id.stop_time == datetime.strptime("20210412T151136", datetime_strptime)
    assert sentinel_product_id.absolute_orbit == "037419"
    assert sentinel_product_id.mission_take_id == "046917"
    assert sentinel_product_id.product_unique_id == "218A"
