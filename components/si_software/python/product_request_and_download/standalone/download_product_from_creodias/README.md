# S2 L1C and L2A download service

## Create creodias account
Go to `https://creodias.eu/` and create an account. You don't need a real email, you can use a temporary email or fake email.


## Use cases for single product download

Only S2_L1C, S2_L2A, and S2_GLC products are handled for single product download for now.

Download S2 L1C product : 
```
./download_product_from_creodias.py \
    --product_id S2B_MSIL1C_20210305T102809_N0209_R108_T32TLR_20210305T124529.SAFE \
    --output_dir test_download \
    --temp_dir test_download_temp
```


Download S2 L2A product : 
```
./download_product_from_creodias.py \
    --product_id S2B_MSIL2A_20210305T102809_N0214_R108_T32TLR_20210305T132245.SAFE \
    --output_dir test_download \
    --temp_dir test_download_temp
```


Download S2 GLC product : 
```
./download_product_from_creodias.py \
    --product_id S2GLC_T32TLR_2017 \
    --output_dir test_download \
    --temp_dir test_download_temp
```



## Use cases for timeseries download

Only S2_L1C and S2_L2A products are handled for timeseries for now.

Download S2 L1C products for tile 32TLR between 2020-03-01 and 2020-04-01 : 
```
./download_product_from_creodias.py \
    --product_type S2_L1C \
    --date_min '2020-03-01' \
    --date_max '2020-04-01' \
    --additional_info 'tile_id:32TLR' \
    --output_dir test_download \
    --temp_dir test_download_temp
```


Download S2 L2A products for tile 32TLR between 2020-03-01 and 2020-04-01 : 
```
./download_product_from_creodias.py \
    --product_type S2_L2A \
    --date_min '2020-03-01' \
    --date_max '2020-04-01' \
    --additional_info 'tile_id:32TLR' \
    --output_dir test_download \
    --temp_dir test_download_temp
```
