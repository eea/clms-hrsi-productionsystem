set -e


/home/eouser/cosims/components/si_software/python/si_software_part2/make_arlie_db.py \
    --arlie_metadata_dir /home/eouser/arlie_generation/arlie_metadata \
    --temp_dir /home/eouser/arlie_generation/db_arlie \
    --docker_file_dir $(pwd)
