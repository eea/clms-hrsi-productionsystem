export PYTHONPATH=$(pwd)/../..:$PYTHONPATH
source /work/ALT/swot/aval/neige/cosims_env/cosims_install/activate
test_l1c_service_client.py --ndl_max=100 --nprocs=24 --temp_dir=/work/scratch/jugierr/test_dl --verbose=2
