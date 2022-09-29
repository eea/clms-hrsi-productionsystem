export PYTHONPATH=$(pwd)/../..:$PYTHONPATH
./test_l1c_service_client.py --ndl_max=6 --temp_dir=test_dl --verbose=2
