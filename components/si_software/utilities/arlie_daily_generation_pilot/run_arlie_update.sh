set -e

D=$(date --date="1 day ago")
previous_day=$(date -d "$D" '+%d')
previous_month=$(date -d "$D" '+%m')
previous_year=$(date -d "$D" '+%Y')


/home/eouser/cosims/components/si_software/python/si_software_part2/make_arlie_update.py \
    --arlie_metadata_dir /home/eouser/arlie_generation/arlie_metadata \
    --rlie_data_dir /home/eouser/arlie_generation/rlie_data \
    --day_to_retrieve ${previous_year}-${previous_month}-${previous_day} \
    --compute_arlie_docker_image 'si_software_part2:latest' \
    --export_arlie_docker_image 'si_arlie_export:2_21' \
    --temp_dir /home/eouser/arlie_generation/temp \
    --nprocs 4
