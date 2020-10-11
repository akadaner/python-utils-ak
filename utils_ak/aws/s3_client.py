import boto3
import os


class S3Client(object):
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key or None
        self.api_secret = api_secret or None
        self.client = boto3.client('s3',
                                   aws_access_key_id=self.api_key,
                                   aws_secret_access_key=self.api_secret)

    def _file_exists_on_s3(self, file_path, bucket):
        from botocore.errorfactory import ClientError
        """
        Returns True if a file at 'file_path' exists in 'bucket'
        :param file_path: path to target file
        :param bucket: bucket os S3
        """
        try:
            self.client.head_object(Bucket=bucket, Key=file_path)
        except ClientError:
            return False
        except Exception as e:
            raise (e)
        else:
            return True

    def download(self, path, bucket, local=None, overwrite=False, remove=False):
        """
        Downloads a file or a folder from a specified bucket on S3.
        Preserves hierarchy.
        :param path: path to file/folder WITHIN 'bucket'
        :param local: local folder to copy to
        :param bucket: bucket on S3
        :param overwrite: overwrites local files if True
        :param remove: if True, removes the origin file
        """
        local = local or os.getcwd()
        path = path.strip(os.sep)

        paginator = self.client.get_paginator('list_objects')
        for p in paginator.paginate(Bucket=bucket, Delimiter=os.sep, Prefix=path):
            if p.get('CommonPrefixes') is not None:
                for subdir in p.get('CommonPrefixes'):
                    self.download(subdir.get('Prefix') + os.sep, local, bucket)
            if p.get('Contents') is not None:
                for file in p.get('Contents'):
                    fk = file.get('Key')
                    local_path = os.path.join(local, fk)
                    if not os.path.exists(os.path.dirname(local_path)):
                        os.makedirs(os.path.dirname(local_path))
                    if not os.path.exists(local_path) or overwrite:
                        self.client.download_file(bucket, fk, local_path)
                    if remove:
                        self.client.delete_object(Bucket=bucket, Key=fk)

    def upload(self, path, bucket, root=None, target='default', overwrite=False, remove=False):
        """
        Uploads a file or a folder to a specified target location on a bucket on S3.
        Preserves hierarchy.
        :param path: path to a local file/folder WITHIN "top"
        :param root: path to folder where the hierarchy should be copied from (e.g. ../mnt/granular_storage)
        :param target: target folder withing the 'bucket' to copy to
        :param bucket: bucket on S3
        :param overwrite: overwrites files if True
        :param remove: if True, removes the origin file
        """
        # path = path.strip(os.sep)
        len_top = 0
        try:
            target = target.strip(os.sep) + os.sep
            root = root or os.sep + root.strip(os.sep) + os.sep
            path = root + path
            len_top += len(root)
        except:
            pass

        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    local_path = os.path.join(root, file)
                    local_path_rel = local_path[len_top:]
                    file_exists = self._file_exists_on_s3(target + local_path_rel, bucket)
                    if file_exists:
                        if overwrite:
                            self.client.upload_file(local_path, bucket, target + local_path_rel)
                    else:
                        self.client.upload_file(local_path, bucket, target + local_path_rel)
                    if remove:
                        os.remove(os.path.join(root, file))  # Remove files to empty folders
            if remove:  # Remove folders
                for root, dirs, files in os.walk(path):
                    for dir in dirs:
                        os.rmdir(os.path.join(root, dir))
                os.rmdir(root)

        elif os.path.isfile(path):
            file = path[path.rfind(os.sep) + 1:]
            if self._file_exists_on_s3(target + file, bucket):
                if overwrite:
                    self.client.upload_file(path, bucket, target + file)
            else:
                self.client.upload_file(path, bucket, target + file)
            if remove:
                os.remove(path)
        else:
            raise Exception(f'Unknown path: {path}')

    def delete(self, target, bucket):
        """
        :param bucket: bucket name on S3
        :param target: target directory/file to delete
        """
        target = target.strip(os.sep)

        paginator = self.client.get_paginator('list_objects')
        for p in paginator.paginate(Bucket=bucket, Delimiter=os.sep, Prefix=target):
            if p.get('CommonPrefixes') is not None:
                for subdir in p.get('CommonPrefixes'):
                    self.delete(target=subdir.get('Prefix') + os.sep, bucket=bucket)
            if p.get('Contents') is not None:
                delete_dict = dict(Objects=[])
                for file in p.get('Contents'):
                    fk = file.get('Key')
                    delete_dict['Objects'].append(dict(Key=fk))
                    if len(delete_dict['Objects']) > 100:  # There is a limit on chunk size in delete_objects
                        self.client.delete_objects(Bucket=bucket, Delete=delete_dict)
                        delete_dict = dict(Objects=[])
                if len(delete_dict['Objects']) > 0:
                    self.client.delete_objects(Bucket=bucket, Delete=delete_dict)

    def get_all_s3_keys(self, bucket, prefix=None, without_prefix=False):
        """
            Get a list of all keys in an S3 bucket.
            :param bucket: bucket name on S3
            :param prefix: prefix path
        """
        kwargs = {'Bucket': bucket}
        if prefix is not None:
            kwargs['Prefix'] = prefix

        while True:
            resp = self.client.list_objects_v2(**kwargs)
            if 'Contents' in resp:
                for obj in resp['Contents']:
                    key = obj['Key']
                    if without_prefix and os.path.normpath(key) == os.path.normpath(prefix):
                        continue
                    yield key

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
