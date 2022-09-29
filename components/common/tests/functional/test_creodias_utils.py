import logging
from ....common.python.util.creodias_util import CreodiasUtil
from ....common.python.util.datetime_util import DatetimeUtil
from ....common.python.database.model.job.fsc_rlie_job import FscRlieJob
from ....common.python.sentinel.sentinel1_product import Sentinel1Product
from ....common.python.sentinel.sentinel2_product import Sentinel2Product
#from ....common.python.util.eea39_util import Eea39Util

def test_creaodias_utils_s2_request():
    """Test that CreodiasUtils access correctly to S2 products."""

    # Variables definition
    mission_type = "s2"
    #geometry = Eea39Util.get_simplified_wkt()
    geometry = "POLYGON((-1.6676811450000082 46.668060706020384,32.2609223475 50.62881003112321,33.420443715000005 47.28719033529018,-2.7263745675000095 39.966352176193794,-1.6676811450000082 46.668060706020384))"
    date_min = DatetimeUtil.fromRfc3339("2021-03-25T00:00:00.000000Z")
    date_max = DatetimeUtil.fromRfc3339("2021-03-25T23:59:59.000000Z")
    max_requested_pages = None

    logger = logging.getLogger("test_creaodias_utils_s2_request")

    # Request new input products in the DIAS catalogue
    # TODO [major] : skip DIAS API test while we can't reach it from outside the 
    # DIAS network
    assert True
    """ s2_product_list = CreodiasUtil().request(
        logger,
        mission_type,
        geometry,
        date_min,
        date_max,
        # Pass the max number of pages to request only if no min date is defined.
        max_requested_pages=max_requested_pages
        if date_min is None else None)
    print(len(s2_product_list))
    for s2_product in s2_product_list:
        print(s2_product)
        print(s2_product.snow_cover)
        assert s2_product is not None

    # Create an FscRlieJob for each Sentinel2 L1C product list
    jobs = []
    for s2_product in s2_product_list:
        assert isinstance(s2_product, Sentinel2Product)
        # TODO add a constructor base on sentinel2_product objects
        jobs.append(FscRlieJob(
                tile_id=s2_product.tile_id,
                l1c_id=s2_product.product_id,
                l1c_path=s2_product.product_path,
                l1c_cloud_cover=s2_product.cloud_cover,
                l1c_snow_cover=s2_product.snow_cover,
                measurement_date=s2_product.measurement_date,
                l1c_esa_creation_date=s2_product.esa_creation_date,
                # There is also the 'update' param that we don't use.
                l1c_dias_publication_date=s2_product.dias_publication_date))
    print(len(jobs))
    for job in jobs:
        print(job)
        assert job is not None """

def test_creaodias_utils_s1_request():
    """Test that CreodiasUtils access correctly to S1 products."""

    # Variables definition
    mission_type = "s1"
    #geometry = Eea39Util.get_simplified_wkt()
    geometry = "POLYGON((-1.6676811450000082 46.668060706020384,32.2609223475 50.62881003112321,33.420443715000005 47.28719033529018,-2.7263745675000095 39.966352176193794,-1.6676811450000082 46.668060706020384))"
    date_min = DatetimeUtil.fromRfc3339("2021-03-25T00:00:00.000000Z")
    date_max = DatetimeUtil.fromRfc3339("2021-03-25T23:59:59.000000Z")
    max_requested_pages = None

    logger = logging.getLogger("test_creaodias_utils_s1_request")

    # Request new input products in the DIAS catalogue
    # TODO [major] : skip DIAS API test while we can't reach it from outside the 
    # DIAS network
    assert True
    """ s1_product_list = CreodiasUtil().request(
        logger,
        mission_type,
        geometry,
        date_min,
        date_max,
        # Pass the max number of pages to request only if no min date is defined.
        max_requested_pages=max_requested_pages
        if date_min is None else None,
        get_manifest=True)
    print(len(s1_product_list))
    for s1_product in s1_product_list:
        assert isinstance(s1_product, Sentinel1Product)
        print(s1_product.product_id)
        print(s1_product.manifest.sliceNumber)
        print(s1_product.manifest.totalSlices)
        print(s1_product.other_metadata.timeliness)
    # the following assertion should remains true as we use the publication date to query data
    assert len(s1_product_list) == 30 """
