import logging
from ....common.python.util.datetime_util import DatetimeUtil
from ....common.python.util.hrsi_product import HrsiProduct
from ....common.python.util.hrsi_util import HrsiUtil

def test_hrsi_utils_fsc_request():
    """Test that HrsiUtil access correctly to FSC products."""

    # Variables definition
    product_type = "fsc"
    geometry = "POLYGON((-1.6676811450000082 46.668060706020384,32.2609223475 50.62881003112321,33.420443715000005 47.28719033529018,-2.7263745675000095 39.966352176193794,-1.6676811450000082 46.668060706020384))"
    date_min = DatetimeUtil.fromRfc3339("2021-05-20T00:00:00.000000Z")
    date_max = DatetimeUtil.fromRfc3339("2021-05-21T23:59:59.000000Z")
    max_requested_pages = None

    logger = logging.getLogger("test_hrsi_utils_fsc_request")
    other_params={'productIdentifier':'%FSC_%'}
    other_params={'mission':'S2'}

    """ # Request new input products in the DIAS catalogue
    hrsi_product_list = HrsiUtil().request(
        logger,
        product_type,
        geometry,
        date_min,
        date_max,
        other_params=other_params,
        # Pass the max number of pages to request only if no min date is defined.
        max_requested_pages=max_requested_pages
        if date_min is None else None)
    assert len(hrsi_product_list) != 0
    for hrsi_product in hrsi_product_list:
        print(hrsi_product.product_id, hrsi_product.tile_id)
        assert isinstance(hrsi_product, HrsiProduct)
        assert hrsi_product.product_type == "FSC"
        assert hrsi_product.mission_type == "S2"
        assert hrsi_product.gml_geometry != None
    # the following assertion should remains true as we use the publication date to query data
    #assert len(hrsi_product_list) == 5 """

def test_hrsi_utils_rlie_request():
    """Test that HrsiUtil access correctly to RLIE products."""

    # Variables definition
    product_type = "rlie"
    geometry = "POLYGON((-1.6676811450000082 46.668060706020384,32.2609223475 50.62881003112321,33.420443715000005 47.28719033529018,-2.7263745675000095 39.966352176193794,-1.6676811450000082 46.668060706020384))"
    date_min = DatetimeUtil.fromRfc3339("2021-05-20T00:00:00.000000Z")
    date_max = DatetimeUtil.fromRfc3339("2021-05-21T23:59:59.000000Z")
    max_requested_pages = None
    other_params={'productIdentifier':'%RLIE_%'}

    logger = logging.getLogger("test_hrsi_utils_rlie_request")

    """ # Request new input products in the DIAS catalogue
    hrsi_product_list = HrsiUtil().request(
        logger,
        product_type,
        geometry,
        date_min,
        date_max,
        other_params=other_params,
        # Pass the max number of pages to request only if no min date is defined.
        max_requested_pages=max_requested_pages
        if date_min is None else None)
    assert len(hrsi_product_list) != 0
    for hrsi_product in hrsi_product_list:
        print(hrsi_product.product_id, hrsi_product.tile_id)
        assert isinstance(hrsi_product, HrsiProduct)
        assert hrsi_product.product_type == "RLIE"
        assert hrsi_product.gml_geometry != None
    # the following assertion should remains true as we use the publication date to query data
    #assert len(hrsi_product_list) == 5
 """