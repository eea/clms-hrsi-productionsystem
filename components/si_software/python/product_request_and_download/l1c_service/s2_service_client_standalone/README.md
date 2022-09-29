# S2 L1C and L2A download service

## install rclone

curl --silent --remote-name  https://downloads.rclone.org/v1.50.2/rclone-v1.50.2-linux-amd64.zip; \
    unzip rclone-v1.50.2-linux-amd64.zip; \
    sudo mv rclone-v1.50.2-linux-amd64/rclone /usr/local/bin/; \
    rm -Rf rclone-v1.50.2-linux-amd64*
    
## configure rclone to access COSIMS test environment bucket

Add bucket information to ~/.config/rclone/rclone.conf file (information not provided here because it is critical)


## test tool

./s2_service_client_standalone.py --product_id S2B_MSIL2A_20210204T115219_N0214_R123_T28SEA_20210204T142150 --output_dir test --verbose 1


