set -e

for ii in '31TCH' '31TGL' '26SPG' '27WWM' '31TGM' '32TLR' '33TYN' '33VUF' '35VPH' '35WNP'
do
    mkdir -p temp/$ii
    docker run --rm \
        -v $(pwd):/workdir \
        -v ~/.config/rclone:/rclone_config \
        si_software:latest chain_processing_request_old.py \
        --tile_id=$ii \
        --date_min='2018-09-01T00:00:00' \
        --date_max='2019-09-01T00:00:00' \
        --output_dir='gdrive:validation_data/cosims_ts/'$ii \
        --output_dir_rclone_config_file='/rclone_config/rclone.conf' \
        --static_data_path='bar:' \
        --static_data_rclone_config_file='/rclone_config/rclone.conf' \
        --temp_dir='/workdir/temp/'$ii \
        --nprocs=2 --keep_l2a
done 
