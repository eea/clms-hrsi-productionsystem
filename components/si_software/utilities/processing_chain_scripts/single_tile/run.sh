mkdir -p temp/$1
docker run --rm \
    -v $(pwd):/workdir \
    -v ~/.config/rclone:/rclone_config \
    si_software:latest chain_processing_request_old.py \
    --tile_id=$1 \
    --date_min='2020-05-01T00:00:00' \
    --output_dir='/workdir/out/'$1 \
    --output_dir_rclone_config_file='/rclone_config/rclone.conf' \
    --static_data_path='bar:' \
    --static_data_rclone_config_file='/rclone_config/rclone.conf' \
    --temp_dir='/workdir/temp/'$1 \
    --nprocs=2 --keep_l2a --single_tile --keep_temp_dir_error
