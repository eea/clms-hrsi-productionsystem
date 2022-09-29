import logging
from ....common.python.util.esa_util import EsaUtil
from ....common.python.util.datetime_util import DatetimeUtil
from ...python.util.log_util import temp_logger

def test_esa_utils_s2_request():
    """Test that CreodiasUtils access correctly to S2 products."""

    # Variables definition
    s2_id = ["S2B_MSIL1C_20210325T102639_N0209_R108_T32ULU_20210325T141549","S2B_MSIL1C_20210325T102639_N0209_R108_T32ULU_20210325T141549"]

    logger = logging.getLogger("test_esa_utils_s2_request")

    # Request new input products in the DIAS catalogue
    s2_product_list = EsaUtil().request(s2_id)
    print(s2_product_list)
    assert s2_product_list is not None