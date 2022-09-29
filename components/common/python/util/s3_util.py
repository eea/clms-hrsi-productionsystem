# The S3 conventions can be a bit tricky sometime. In particular, about the
# notions of Object, Key, Prefix and Delimiter. Here is a blog post explaining
# that: https://realguess.net/2014/05/24/amazon-s3-delimiter-and-prefix/

import os
import pathlib

import boto3
import botocore
from logging import Logger

from .exceptions import CsiInternalError, CsiExternalError

class S3Util(object):
    '''S3 storage access utility functions'''

    @staticmethod
    def get_s3(endpoint_url: str, access_key: str, secret_key: str):
        '''
        Utility function establishing a connection with an s3 endpoint storage,
        and storing it in an object which can then be called to access the storage 
        content.

        :param endpoint_url: url leading to the s3 endpoint storage.
        :param access_key: access key to connect to the s3 storage endpoint.
        :param access_key: secret key to connect to the s3 storage endpoint.
        '''
        # TODO it is not easy, be it would be great if we can catch if an endpoint
        # is a valid HTTP endpoint but no a S3 storage one. If it is the case we
        # will have some error while trying to connect to bucket or object but the
        # message won't be clear.

        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url
            )
        return s3_resource


    @staticmethod
    def check_bucket(s3_resource, bucket: str):
        '''
        Utility function to check if a bucket exists on an S3 endpoint storage.
        
        :param s3_resource: s3 resource object.
        :param bucket: name of the bucket to test.
        '''
        # We need to access to the S3 client for this resource to reference
        # exceptions and some meta info below.
        s3_client = s3_resource.meta.client
        try:
            s3_client.head_bucket(Bucket=bucket)
        except s3_client.exceptions.ClientError as s3_error:
            code = s3_error.response['Error']['Code']
            if code == '403':
                message = (
                    f'bad credentials given to connect to the "{bucket}" '
                    f'bucket with the S3 endpoint URL '
                    f'{s3_client.meta.endpoint_url}"'
                )
                internal_error = CsiInternalError('Bad S3 credentials', message)
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise internal_error from s3_error
            elif code == '404':
                message = (
                    f'the bucket "{bucket}" doesn\'t exist in the S3 storage '
                    f'which endpoint URL is "{s3_client.meta.endpoint_url}" and '
                    f'with the given credentials'
                )
                internal_error = CsiInternalError('S3 bucket not found', message)
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise internal_error from s3_error
            elif code == '400':
                message = (
                    f'failed to do the request to check if bucket "{bucket}" '
                    f'exists, with endpoint URL "{s3_client.meta.endpoint_url}" '
                    f'and with the given credentials. The HTTP error code is '
                    f'{code}. This can happen when credentials are given but set '
                    f'to empty strings (for both access and secret keys).'
                )
                error = CsiExternalError('S3 HTTP error', message)
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise error from s3_error
            else:
                message = (
                    f'failed to do the request to check if bucket "{bucket}" '
                    f'exists, with endpoint URL "{s3_client.meta.endpoint_url}" '
                    f'and with the given credentials. The HTTP error code is '
                    f'{code}. Could not tell what is going one.'
                )
                internal_error = CsiExternalError('S3 HTTP error', message)
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise internal_error from s3_error
        except botocore.exceptions.EndpointConnectionError as s3_error:
            message = (
                f'could not connect to the S3 endpoint URL '
                f'"{s3_client.meta.endpoint_url}", either there is an error in the '
                f'URL or the endpoint is not responding'
            )
            external_error = CsiExternalError('S3 endpoint error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise external_error from s3_error
        except Exception as error:
            message = (
                f'an unexpected error occured during the check of the bucket '
                f'{bucket}, raise an external error so that it might be tried '
                f'again in case it is due to a temporary issue'
            )
            external_error = CsiExternalError('unknown S3 bucket error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise external_error from error


    @staticmethod
    def object_exists(s3_resource, bucket, object_in_bucket):
        '''
        Utility function to check if an object exists on an S3 endpoint storage.
        
        :param s3_resource: s3 resource object.
        :param bucket: name of the bucket in which is stored the object.
        :param object_in_bucket: path leading to the object to test on the bucket.
        '''

        s3_client = s3_resource.meta.client
        S3Util.check_bucket(s3_resource, bucket)
        try:
            # This command will fail if object doesn't exist on the S3 bucket.
            s3_resource.Object(bucket, object_in_bucket).load()
            object_exists = True
        except s3_client.exceptions.ClientError as s3_error:
            code = s3_error.response['Error']['Code']
            if code == '404':
                object_exists = False
            else:
                message = (
                    f'an unexpected error occured during the check of object '
                    f'{object_in_bucket} existence in the bucket {bucket}, raise an '
                    f'external error so that it might be tried again in case it is due '
                    f'to a temporary issue'
                )
                external_error = CsiExternalError('unknown S3 error', message)
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise external_error from error

        except Exception as error:
            message = (
                f'an unexpected error occured during the check of object '
                f'{object_in_bucket} existence in the bucket {bucket}, raise an '
                f'external error so that it might be tried again in case it is due '
                f'to a temporary issue'
            )
            external_error = CsiExternalError('unknown S3 error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise external_error from error
        return object_exists


    @staticmethod
    def download_file_from_bucket(
        s3_resource, bucket: str, object_in_bucket: str, local_file_name: str
    ):
        '''
        Utility function to download a file stored on an S3 endpoint storage, 
        into a specified path on a local worker machine.
        
        :param s3_resource: s3 resource object.
        :param bucket: name of the bucket in the s3 endpoint storage.
        :param object_in_bucket: path leading to the object to download in the bucket.
        :param local_file_name: path under which will be saved the downloaded file.
        '''

        # We need to access to the S3 client for this resource to reference
        # exceptions and some meta info below.
        s3_client = s3_resource.meta.client
        S3Util.check_bucket(s3_resource, bucket)
        try:
            # This command will fail if object doesn't exist on the S3 bucket.
            s3_resource.Object(bucket, object_in_bucket).load()
        except s3_client.exceptions.ClientError as s3_error:
            code = s3_error.response['Error']['Code']
            if code == '404':
                message = (
                    f'the object "{object_in_bucket}" doesn\'t exist in the bucket '
                    f'"{bucket}" in the S3 storage which endpoint URL is '
                    f'"{s3_client.meta.endpoint_url}" and with the given '
                    f'credentials'
                )
                external_error = CsiExternalError('S3 object not found', message)
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise external_error from s3_error
            else:
                message = (
                    f'an unknown error occured while downloading the object '
                    f'"{object_in_bucket}" from the bucket "{bucket}" in the S3 '
                    f'storage which endpoint URL is '
                    f'"{s3_client.meta.endpoint_url}"; HTTP status = {code}'
                )
                external_error = CsiExternalError('S3 object download error', message)
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise external_error from s3_error

        except botocore.exceptions.EndpointConnectionError as s3_error:
            message = (
                f'an unknown error occured while downloading the object '
                f'"{object_in_bucket}" from the bucket "{bucket}" in the S3 '
                f'storage which endpoint URL is '
                f'"{s3_client.meta.endpoint_url}"'
            )
            external_error = CsiExternalError('S3 object download error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise external_error from s3_error

        except botocore.exceptions.ReadTimeoutError as s3_error:
            message = (
                f'request timed out wile attempting to read the object '
                f'"{object_in_bucket}" from the bucket "{bucket}" in the S3 '
                f'storage which endpoint URL is '
                f'"{s3_client.meta.endpoint_url}"'
            )
            external_error = CsiExternalError('S3 object download error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise external_error from s3_error


        # Everything is fine on the S3 side, we can launch the download
        try:
            s3_resource.Bucket(bucket).download_file(
                object_in_bucket, local_file_name
                )
        except PermissionError as error:
            message = (
                f'could not write the downloaded file from S3 storage due to '
                f'permission error for "{local_file_name}"'
            )
            internal_error = CsiInternalError('File write error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise internal_error from error
        except IsADirectoryError as error:
            message = (
                f'could not write the downloaded file from S3 storage because '
                f'the target is a directory "{local_file_name}"'
            )
            internal_error = CsiInternalError('File write error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise internal_error from error
        except Exception as error:
            message = (
                f'an unexpected error occured during the download of '
                f'{object_in_bucket} from the bucket {bucket}, raise an '
                f'external error so that it might be tried again in case '
                f'it is due to a temporary issue'
            )
            external_error = CsiExternalError('unknown S3 download error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise external_error from error


    @staticmethod
    def compute_local_file_path(
        s3_object: str, local_directory_name: str, objects_prefix: str = 'None', logger: Logger = None
    ):
        '''
        Utility function computing the local path on the worker of an object 
        stored on an S3 endpoint storage.

        :param s3_object: path leading to the object to download on the bucket.
        :param local_directory_name: path of the directory under which will be saved the file to download.
        :param objects_prefix: bucket path prefix which won't be added to the "local_directory_name".
        :param logger: logger object used to display messages.
        '''

        # If the request concerns an unitary file, we have no object_prefix so we use the path on the bucket
        if objects_prefix == 'None':
            objects_prefix = s3_object

        prefix_without_ending_slash = objects_prefix.rstrip('/')
        prefix_parent_dir_with_ending_slash = f'{os.path.dirname(prefix_without_ending_slash)}/'
        name_without_prefix = S3Util.remove_prefix(s3_object, prefix_parent_dir_with_ending_slash)
        local_file_path = os.path.join(local_directory_name, name_without_prefix)
        # Raise a warning if the file already exists on worker
        if os.path.isfile(local_file_path):
            logger.warning(f'The file \'{local_file_path}\' already exists on the worker.')

        local_directory_path = os.path.dirname(local_file_path)
        try:
            pathlib.Path(local_directory_path).mkdir(parents=True, exist_ok=True)
        except PermissionError as error:
            message = (
                f'could not write the downloaded object from S3 storage due to '
                f'permission error during creation of its parent directory '
                f'"{local_directory_path}"'
            )
            internal_error = CsiInternalError('File write error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise internal_error from error
        return local_file_path


    @staticmethod
    def remove_prefix(text: str, prefix: str) -> str:
        '''
        Utility function removing a prefix from a given string.

        :param text: string to which we should remove the prefix.
        :param prefix: prefix to be removed from the given string.
        '''

        if text.startswith(prefix):
            return text[len(prefix):]
        return text


    @staticmethod
    # Code from https://stackoverflow.com/a/56267603
    def download_prefixed_objects(
        s3_resource, bucket: str, objects_prefix: str, local_directory_name: str, logger: Logger = None
    ):
        '''
        Utility function to download a set of objects which have a path matching 
        a prefix on an S3 endpoint storage, into a specified folder on a local 
        worker machine.

        :param s3_resource: s3 resource object.
        :param bucket: name of the bucket in the s3 endpoint storage.
        :param objects_prefix: path prefix leading to the objects to download on the bucket.
        :param local_directory_name: path of the directory under which will be saved the downloaded file.
        :param logger: logger object used to display messages.
        '''

        S3Util.check_bucket(s3_resource, bucket)
        s3_client = s3_resource.meta.client

        s3_objects = []

        next_token = ''
        base_kwargs = {
            'Bucket': bucket,
            'Prefix': objects_prefix,
        }

        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})

            try:
                results = s3_client.list_objects_v2(**kwargs)
            except Exception as error:
                message = (
                    f'an unexpected error occured during getting the list of '
                    f'objects with prefix {objects_prefix} in the bucket {bucket}, '
                    f'raise an external error so that it might be tried again in '
                    f'case it is due to a temporary issue'
                )
                external_error = CsiExternalError('unknown S3 error', message)
                # Use the 'raise e1 from e2' form to keep the trace of the error.
                raise external_error from error

            s3_object_contents = results.get('Contents')

            if s3_object_contents is None:
                message = (
                    f'there is no object with prefix "{objects_prefix}" in the bucket '
                    f'"{bucket}" in the S3 storage which endpoint URL is '
                    f'"{s3_client.meta.endpoint_url}" and with the given '
                    f'credentials'
                )
                external_error = CsiExternalError('S3 object not found', message)
                raise external_error

            # Actually, the object string is in the "Key" field of the contents
            # returned by the request above.
            s3_objects = [
                s3_object.get('Key')
                for s3_object in s3_object_contents
            ]

            # We don't want to keep directories in this list, i.e. objects which
            # names end with a "/"
            s3_objects = [
                s3_object
                for s3_object in s3_objects
                if s3_object[-1] != '/'
            ]

            next_token = results.get('NextContinuationToken')

        # Actually download all the s3_objects
        for s3_object in s3_objects:
            # if
            #   objects_prefix = 'some/path/to/object'
            # and
            #   local_directory_name = '/some/local/dir'
            # in the end we want to have all objects in this directory
            #   '/some/local/dir/object'

            # Compute the local file path
            local_file_path = S3Util.compute_local_file_path(
                s3_object, local_directory_name, objects_prefix, logger)

            # Download the file from bucket
            S3Util.download_file_from_bucket(s3_resource, bucket, s3_object, local_file_path)


    @staticmethod
    def upload_directory(
        s3_resource, bucket: str, local_directory_name: str, s3_destination: str
    ):
        '''
        Utility function uploading a given directory stored on a local worker 
        machine into a specified location on an S3 endpoint storage.
        
        :param s3_resource: s3 resource object.
        :param bucket: name of the bucket in the s3 endpoint storage.
        :param local_directory_name: path of the directory under which is saved the directory to upload.
        :param s3_destination: path under which will be saved the directory on the bucket.
        '''

        S3Util.check_bucket(s3_resource, bucket)
        s3_client = s3_resource.meta.client

        if not os.path.isdir(local_directory_name):
            raise CsiInternalError(
                'Directory not found',
                f'could not find local directory ({local_directory_name}) to upload to S3 bucket')

        for root, _, files in os.walk(local_directory_name):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(
                    local_path,
                    os.path.dirname(local_directory_name)
                    )
                s3_object_path = os.path.join(s3_destination, relative_path)
                try:
                    s3_client.upload_file(local_path, bucket, s3_object_path)
                except Exception as error:
                    message = (
                        f'an unexpected error occured during the upload of '
                        f'{local_directory_name} to the bucket {bucket}, raise an '
                        f'external error so that it might be tried again in case '
                        f'it is due to a temporary issue'
                    )
                    external_error = CsiExternalError('unknown S3 upload error', message)
                    # Use the 'raise e1 from e2' form to keep the trace of the error.
                    raise external_error from error

    @staticmethod
    def upload_file(
        s3_resource, bucket: str, local_file_name: str, s3_destination_filepath: str
    ):
        '''
        Utility function uploading a given directory stored on a local worker 
        machine into a specified location on an S3 endpoint storage.
        
        :param s3_resource: s3 resource object.
        :param bucket: name of the bucket in the s3 endpoint storage.
        :param local_file_name: path of the input file
        :param s3_destination_filepath: filepath to store file on bucket
        '''

        S3Util.check_bucket(s3_resource, bucket)
        s3_client = s3_resource.meta.client

        if not os.path.isfile(local_file_name):
            raise CsiInternalError(
                'File not found',
                f'could not find local file ({local_file_name}) to upload to S3 bucket')

        try:
            s3_client.upload_file(local_file_name, bucket, s3_destination_filepath)
        except Exception as error:
            message = (
                f'an unexpected error occured during the upload of '
                f'{local_file_name} to the bucket {bucket}, raise an '
                f'external error so that it might be tried again in case '
                f'it is due to a temporary issue'
            )
            external_error = CsiExternalError('unknown S3 upload error', message)
            # Use the 'raise e1 from e2' form to keep the trace of the error.
            raise external_error from error
