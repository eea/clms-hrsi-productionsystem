import requests
import os


def test_sip_aux_files():
    # Request the data list from the public url (read_only) to the hidden_value bucket
    url_base = 'https://s3.waw2-1.cloudferro.com/swift/v1/AUTH_hidden_value/hidden_value/'
    data = requests.get(url_base)

    # Extract a list of file path present in the bucket
    list_file = [x.decode('UTF-8') for x in data.iter_lines()]

    # A request can only return up to 10 000 object so we need multiple request to parse the whole bucket.
    total_list = list_file
    while len(list_file) >= 10000:
        # Add a starting marker to the url to start the request from the last found file
        url = url_base + '?marker=' + list_file[-1]
        data = requests.get(url)
        list_file = [x.decode('UTF-8') for x in data.iter_lines()]
        total_list += list_file

    # Find all the list_files.txt files defining the files to find
    list_files_paths = [os.path.join(x[0], x[-1][0]) for x in os.walk(os.path.join('tests', 'infra', 'buckets')) if
                        x[-1] == ['list_files.txt'] and ('TILE_ID' not in x[0] or 'TILE_ID' in x[1])]

    # Iterate trough all the list_files.txt files
    files_to_find = []
    for file_path in list_files_paths:
        with open(file_path, 'r') as f:
            # Extract a list of file path
            sub_files_to_find = f.read().splitlines()

            # The organisation of the files in the bucket is not the same as defined in the test folder
            # so we need to convert the path of each file to match with the bucket
            # Note: The bucket use UNIX style paths
            base_folder = os.path.split(file_path)[0]
            if 'csi_aux' + os.sep in base_folder:
                base_folder = base_folder.split('csi_aux' + os.sep)[-1].split(os.sep)
            else:
                base_folder = base_folder.split('sip-aux' + os.sep)[-1].split(os.sep)
            base_folder = '/'.join(base_folder)
            sub_files_to_find = ['/'.join([base_folder, x]).replace('/./', '/') for x in sub_files_to_find]
            files_to_find += sub_files_to_find

    # Extract the missing file from the list of file found in the bucket compared to the list extracted from
    # the list_files.txt files
    not_found = set(files_to_find).difference(total_list)
    assert len(not_found) == 0, 'Files not found in the bucket: %s' % ', '.join(list(not_found))
