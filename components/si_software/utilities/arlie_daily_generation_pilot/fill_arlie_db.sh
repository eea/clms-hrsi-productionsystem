set -e

year=2022

/home/eouser/cosims/components/si_software/python/si_software_part2/fill_arlie_db.py --year ${year} --temp_dir /home/eouser/arlie_generation/temp_dir_fill_db
