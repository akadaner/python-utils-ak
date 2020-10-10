import os
import paramiko
import scp
import sys
import glob

from utils.strtypes import cast_unicode


def progress(filename, size, sent):
    sys.stdout.write("%s\'s progress: %.2f%%   \r" % (filename, float(sent) / float(size) * 100))


class SSH(paramiko.SSHClient):
    """
    Wrapper for paramiko ssh client and ssh client.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.args = args
        self.kwargs = kwargs
        self.load_system_host_keys()
        self.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.connect()

    def connect(self):
        super().connect(*self.args, **self.kwargs)

    def put(self, *args, **kwargs):
        with scp.SCPClient(self.get_transport(), progress=progress) as scp_cli:
            scp_cli.put(*args, **kwargs)

    def _unfold_files(self, fns):
        if isinstance(fns, str):
            res = glob.glob(fns, recursive=True)
            return [fn for fn in res if os.path.isfile(fn)]
        elif isinstance(fns, list):
            return sum([self._unfold_files(fn) for fn in fns], [])
        else:
            raise Exception('Unknown wrong file format')

    def put_missing(self, fns, remote_path):
        existing_files = self.ls(remote_path)
        existing_files = [os.path.basename(fn) for fn in existing_files]
        fns = self._unfold_files(fns)
        fns = [fn for fn in fns if os.path.basename(fn) not in existing_files]
        self.put(fns, remote_path=remote_path, recursive=True)

    def get(self, *args, **kwargs):
        with scp.SCPClient(self.get_transport(), progress=progress) as scp_cli:
            scp_cli.get(*args, **kwargs)

    def _format_command(self, command):
        if isinstance(command, list):
            return '; '.join([self._format_command(x) for x in command])
        elif isinstance(command, str):
            return command
        else:
            raise ValueError('Unknown command type')

    def exec(self, command, from_path=None, bufsize=-1, timeout=None, get_pty=False, environment=None, raw_output=False,
             raise_on_error=True):
        if from_path:
            command = [f'cd {from_path}', command]

        command = self._format_command(command)
        stdin, stdout, stderr = self.exec_command(command, bufsize, timeout, get_pty, environment)

        if raw_output:
            return stdin, stdout, stderr

        stdout, stderr = stdout.read(), stderr.read()
        stdout, stderr = cast_unicode(stdout), cast_unicode(stderr)
        if raise_on_error and stderr:
            raise Exception(f'Command failed: {stderr}')
        output = stdout + stderr
        return self._remove_last_new_line(output)

    def run(self, *args, **kwargs):
        return self.exec(*args, **kwargs)

    def _remove_last_new_line(self, s):
        if not s:
            return s
        return s[:-1] if s[-1] == '\n' else s

    def ls(self, path='.', full_path=True):
        stdin, stdout, stderr = self.exec('ls', from_path=path, raw_output=True)
        stdout = cast_unicode(stdout.read())
        stdout = self._remove_last_new_line(stdout)
        res = stdout.split('\n')
        if full_path and path != '.':
            res = [os.path.join(path, x) for x in res]
        return res


if __name__ == '__main__':
    with SSH(hostname='52.197.120.112', port=50121, username='ubuntu', key_filename=r'C:\Users\xiaomi\YandexDisk\IT\Python\jupyter2\paramiko\ssh_keys/qtb_tokyo') as ssh:
        print(ssh.exec(['cd git', 'ls']))  # == ssh.exec('cd git; ls')
        local_fn = r"C:\Users\xiaomi\YandexDisk\IT\Python\jupyter2\paramiko\test.txt"
        ssh.put(local_fn, 'remote.txt')
        print(ssh.exec('ls'))
        ssh.get('remote.txt', 'local.txt')
        print(ssh.ls('git'))

        ssh.put('local_folder', 'remote_folder', recursive=True)
        print(ssh.ls())

        import traceback

        try:
            print(ssh.exec('lsasdfasdf'))
        except:
            traceback.print_exc()

        ssh.put_missing(r"C:\Users\xiaomi\YandexDisk\IT\Python\jupyter2\paramiko\*", 'remote_folder/')
