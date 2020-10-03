import subprocess
import os

from utils_ak.strtypes import cast_unicode


def execute(cmd, async_=False):
    if async_:
        process = subprocess.Popen(cmd, shell=True)
    else:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        return ' '.join([cast_unicode(x) for x in [output, error] if x]), process.returncode


class AwsDumper:
    def __init__(self, local_file_path, bucket, remote_file_dir,  *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.local_file_path = local_file_path
        self.remote_file_dir = remote_file_dir
        self.bucket = bucket

    def tick(self):
        local_file_dir, _ = os.path.split(self.local_file_path)
        message, code = execute(
            f'aws s3 sync {local_file_dir} s3://{self.bucket}/{self.remote_file_dir} '
            f'--exclude="*" --include={self.local_file_path} --delete')

        if code != 0:
            raise Exception(message)

        return message
