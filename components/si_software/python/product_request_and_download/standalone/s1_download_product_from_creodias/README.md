# S1 SLC and GRD download service

## Create creodias account
Go to `https://creodias.eu/` and create an account. You don't need a real email, you can use a temporary email or fake email.


## Use cases for single product download

Only S1_SLC, S1_GRD and S1_GRDCOG products are handled for single product download for now.

Download S1_SLC product : 
```
./s1_download_product_from_creodias.py \
    --product_id S1B_IW_SLC__1SSH_20210324T121113_20210324T121144_026156_031F09_F1F4.SAFE \
    --output_dir test_download \
    --creodias_credentials_u TO COMPLETE \
    --creodias_credentials_p TO COMPLETE \
    --temp_dir test_download_temp
```


Download S1_GRD product : 
```
./s1_download_product_from_creodias.py \
    --product_id S1B_EW_GRDM_1SDH_20210325T070912_20210325T070927_026168_031F68_C234.SAFE \
    --output_dir test_download \
    --creodias_credentials_u TO COMPLETE \
    --creodias_credentials_p TO COMPLETE \
    --temp_dir test_download_temp
```


Download S1_GRDCOG product : 
```
./s1_download_product_from_creodias.py \
    --product_id S1A_IW_GRDH_1SDV_20210316T094107_20210316T094141_037022_045B63_87EA_COG.SAFE \
    --output_dir test_download \
    --creodias_credentials_u TO COMPLETE \
    --creodias_credentials_p TO COMPLETE \
    --temp_dir test_download_temp
```

## Use cases for timeseries download

Only S1_SLC, S1_GRD and S1_GRDCOG products are handled for timeseries for now.

Download S1_SLC products for tile 32TLQ between 2020-03-01 and 2020-04-01 : 
```
./s1_download_product_from_creodias.py \
    --product_type S1_SLC \
    --date_min '2020-03-01' \
    --date_max '2020-04-01' \
    --output_dir test_download \
    --tile_id '32TLQ' \
    --creodias_credentials_u TO COMPLETE \
    --creodias_credentials_p TO COMPLETE \
    --temp_dir test_download_temp \
    --shpfile_path COMPLETE WITH PATH TO s2tiles_eea39.shp
```

Download S1_GRD products for tile 32TLQ between 2020-03-01 and 2020-04-01 : 
```
./s1_download_product_from_creodias.py \
    --product_type S1_GRD \
    --date_min '2020-03-01' \
    --date_max '2020-04-01' \
    --output_dir test_download \
    --tile_id '32TLQ' \
    --creodias_credentials_u TO COMPLETE \
    --creodias_credentials_p TO COMPLETE \
    --temp_dir test_download_temp \
    --shpfile_path COMPLETE WITH PATH TO s2tiles_eea39.shp
```
Download S1_GRDCOG products for tile 32TLQ between 2020-03-01 and 2020-04-01 : 
```
./s1_download_product_from_creodias.py \
    --product_type S1_GRDCOG \
    --date_min '2020-03-01' \
    --date_max '2020-04-01' \
    --output_dir test_download \
    --tile_id '32TLQ' \
    --creodias_credentials_u TO COMPLETE \
    --creodias_credentials_p TO COMPLETE \
    --temp_dir test_download_temp \
    --shpfile_path COMPLETE WITH PATH TO s2tiles_eea39.shp
```
