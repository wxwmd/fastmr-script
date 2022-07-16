import glob
import math
import os
import shlex
import stat
import threading
import time
import xml.etree.ElementTree as ET
from typing import List
from typing import Tuple

import aliyun_backend
import aliyun_util as u
import backend
import constant as c
import ncluster_globals
import util

# default value of logdir root for this backend (can override with set_logdir_root)
DEFAULT_LOGDIR_ROOT = '/ncluster/runs'
TMPDIR = '/tmp/ncluster-{}'.format(util.get_username())  # location for temp files on launching machine

LOG_LEVEL = os.environ.get('NCLUSTER_LOG_LEVEL') if os.environ.get('NCLUSTER_LOG_LEVEL') else 'INFO'
LOG_ORDER = c.LOG_ORDER


class Instance:
    def __init__(self, public_ip, private_ip, vcpunum, meminfo, hostname):
        self.publicIp = public_ip
        self.privateIp = private_ip
        self.vcpunum = int(vcpunum)
        self.meminfo = int(meminfo)
        self.hostname = hostname

    def host_name(self) -> str:
        return self.hostname

    def public_ip(self):
        return self.publicIp

    def private_ip(self):
        return self.privateIp

    def cpu(self):
        return self.vcpunum

    def memory(self):
        return self.meminfo


class Task(backend.Task):
    """ FASTMR task is used to deploy big data components in a set up ALIYUN instance with any image """
    instance: Instance

    @property
    def logdir(self):
        pass

    def __init__(self, name, run_name, install_script, public_ip, username, password, eth=0,
                 **extra_kwargs):
        """
        Initializes Task on top of existing ALIYUN instance. Blocks until instance is ready to execute
        shell commands.

        Args:

        """
        # some message to ssh client
        self.name = name
        self.run_name = run_name
        self.install_script = install_script
        self.public_ip = public_ip
        self.username = username
        self.password = password
        self.ssh_client = u.ssh_to_task_by_password(self)
        self.sftp = None

        # logs
        self.run_counter = 0
        self._cmd = None
        launch_id = util.random_id()
        self.local_scratch = f"{TMPDIR}/{name}-{launch_id}"
        self.remote_scratch = f"{TMPDIR}/{name}-{launch_id}"

        # instance message

        self.ssh_username = 'root'
        self.home_dir = '/' + self.ssh_username

        self.private_ip = self._run_raw('hostname -I')[0].split()[eth]
        self.host_name = self._run_raw('hostname')[0].split('\r')[0]
        # check the number of logical CPUs
        vcpunum = self._run_raw('cat /proc/cpuinfo| grep "processor"| wc -l')[0].split('\r')[0]
        # check the size of memory
        meminfo = math.floor(int(self._run_raw('cat /proc/meminfo | grep "MemTotal"')[0].split()[1]) / 1024)

        self.instance = Instance(self.public_ip, self.private_ip, vcpunum, meminfo, self.host_name)

        os.system('mkdir -p ' + self.local_scratch)
        # can't skip this setup because remote_scratch location changes each rerun
        self._run_raw('mkdir -p ' + self.remote_scratch)
        # check linux type and set up tmux for run_raw_with_no_fail
        stdout_str, _ = self._run_raw('lsb_release -a')
        self._linux_type = u.check_system_version_from_str(stdout_str)
        print("_linux_type = ", self._linux_type)
        self._linux_type = 'centos'
        # update yum
        if self._linux_type == 'centos':
            self._update_yum()

        self._setup_tmux()

    def log(self, *_args, level='DEBUG'):
        LOG_ORDER = c.LOG_ORDER
        if LOG_ORDER.index(level) <= LOG_ORDER.index(LOG_LEVEL):
            super().log(*_args, log_level=level)

    def _update_yum(self):
        if self.exists('/etc/yum.repos.d/CentOS-AppStream.repo.backup') or self.exists(
                '/etc/yum.repos.d/CentOS-Base.repo.backup') or self.exists(
            '/etc/yum.repos.d/CentOS-Linux-AppStream.repo.backup') or self.exists('/etc/yum.repos.d/CentOS-Linux'
                                                                                  '-BaseOS.repo.backup'):
            return
            # AppStream's baseurl have some wrong
        elif self.exists('/etc/yum.repos.d/CentOS-AppStream.repo') or self.exists('/etc/yum.repos.d/CentOS-Base.repo'):
            if self.exists('/etc/yum.repos.d/CentOS-AppStream.repo'):
                self._run_raw('mv /etc/yum.repos.d/CentOS-AppStream.repo /etc/yum.repos.d/CentOS-AppStream.repo.backup')
            if self.exists('/etc/yum.repos.d/CentOS-Base.repo'):
                self._run_raw('mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup')
            self._run_raw(
                'wget -O /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-vault-8.5.2111.repo')

            # CentOS 8.3 change the name of BaseOS.repo
        elif self.exists('/etc/yum.repos.d/CentOS-Linux-AppStream.repo') or self.exists('/etc/yum.repos.d/CentOS-Linux-BaseOS.repo'):
            if self.exists('/etc/yum.repos.d/CentOS-Linux-AppStream.repo'):
                self._run_raw(
                    'mv /etc/yum.repos.d/CentOS-Linux-AppStream.repo /etc/yum.repos.d/CentOS-Linux-AppStream.repo.backup')
            if self.exists('/etc/yum.repos.d/CentOS-Linux-BaseOS.repo'):
                self._run_raw(
                    'mv /etc/yum.repos.d/CentOS-Linux-BaseOS.repo /etc/yum.repos.d/CentOS-Linux-BaseOS.repo.backup')

            self._run_raw(
                'wget -O /etc/yum.repos.d/CentOS-Linux-BaseOS.repo https://mirrors.aliyun.com/repo/Centos-vault-8.5.2111.repo')
        self._run_raw('yum makecache')

    def _oss_download(self, pkg, ARCH, pkg_format, path):
        """Download package from OSS if the busket has the resource"""
        try:

            stdout, stderr = \
                self._run_raw(f'wget -P {path} https://fastmr.oss-cn-shenzhen.aliyuncs.com/{ARCH}/{pkg}{pkg_format}')

        except Exception as e:
            self.log(e)
        if "Error" in stdout:
            self.log(f"download {pkg} fail in {stdout}")
            print(f"download {pkg} fail in {stdout}")
            return stdout

        return self.exists(f'{path}/{pkg}')

    def setup_from_oss(self, pkg, ARCH, pkg_format, path):
        """setup only support the pkg in tar or rpm compression format"""

        if not self.exists(f'{path}/{pkg}{pkg_format}'):
            try:
                result = self._oss_download(pkg, ARCH, pkg_format, path)
                if "Error" in result:
                    return
            except Exception as e:
                self.log(e)
        if pkg_format == '.rpm':
            try:
                self._run_raw(f'yum -y localinstall {path}/{pkg}{pkg_format}')
            except Exception as e:
                self.log(e)
        elif pkg_format == '.tar.gz' or pkg_format == '.tgz':
            try:
                # uncompress the {pkg} to {path}
                self._run_raw(f'cd {path} && tar -zxvf {pkg}{pkg_format}')
            except Exception as e:
                self.log(e)
        else:
            print(f"can't support uncompressing the format in {pkg_format}")

    def setup(self,
              pkg,
              ARCH='x86',
              pkg_format='.tar.gz',
              path='/opt',
              resource='OSS'
              ):
        """only support setting up package from OSS,now"""
        if self.exists(f'{path}/{pkg}'):
            self.log(f"{pkg} already set up in {path} ")
            return
        if resource == 'OSS':
            self.setup_from_oss(pkg, ARCH, pkg_format, path)
        elif resource == 'web':
            self.setup_from_web(pkg, pkg_format, path)
            # TODO:set up package in other ways
            print(f"can't support setting up package from {resource}")
        if self.exists(f'{path}/{pkg}'):
            print()

    def _setup_tmux(self):
        self.log("Setting up tmux...")
        # self.ssh_client_short = u.ssh_to_task(self)
        self.tmux_session = self.name.replace('.', '=')
        self.tmux_window_id = 0
        self.tmux_available_window_ids = [0]

        tmux_cmd = [f'tmux set-option -g history-limit 50000 \; ',
                    # f'set-option -g mouse on \; ', # for ubuntu?
                    f'new-session -s {self.tmux_session} -n 0 -d']

        if self._linux_type == 'ubuntu':
            self._run_raw('sudo apt-get update', ignore_errors=True)
            trying_time = 5
            tmux_installed = False
            for index in range(trying_time):
                stdout, _ = self._run_raw('dpkg -l tmux', ignore_errors=True)
                if 'ii' in stdout:
                    self.log('have tmux installed.')
                    tmux_installed = True
                    break
                else:
                    time.sleep(0.5)
                    self.log(f'({index}/{trying_time}) installing tmux ..')
                    self._run_raw_short('sudo apt-get install -y tmux', ignore_errors=True)

            if not tmux_installed:
                assert True, 'tmux can not be installed.'
            self._run_raw_short('sudo apt-get install -y expect', ignore_errors=True)

        if self._linux_type in ['centos', 'aliyunlinux', 'alibabacloud']:
            self._run_raw('sudo yum check-update', ignore_errors=True)
            self._run_raw('sudo yum install -y tmux expect')

        if not util.is_set("NCLUSTER_NOKILL_TMUX") and not ncluster_globals.should_skip_setup():
            self._run_raw(f'tmux kill-session -t {self.tmux_session}',
                          ignore_errors=True)
        else:
            print(
                "Warning, NCLUSTER_NOKILL_TMUX or skip_setup is set, make sure remote tmux prompt is available or things will hang")

        # if conda is installed, the expect will be conficted
        # https://blog.csdn.net/artistkeepmonkey/article/details/117525202

        ## TODO: sometimes conda default channel fails
        # mv /root/.condarc /root/.condarc.bak
        # if self.exists('/root/miniconda'):
        #   self._run_raw('conda update --force conda -y && conda install -c eumetsat expect -y', ignore_errors=True)

        if not ncluster_globals.should_skip_setup():
            self._run_raw(''.join(tmux_cmd))

        self._can_run = True

    def upload(self, local_fn: str, remote_fn: str = '',
               dont_overwrite: bool = False, show_info=True) -> None:
        """Uploads file to remote instance. If location not specified, dumps it
        into default directory. If remote location has files or directories with the
         same name, behavior is undefined."""

        # support wildcard through glob
        if '*' in local_fn:
            for local_subfn in glob.glob(local_fn):
                self.upload(local_subfn, show_info=show_info)
            return

        if '#' in local_fn:  # hashes also give problems from shell commands
            self.log("skipping backup file {local_fn}")
            return

        if not self.sftp:
            self.sftp = u.call_with_retries(self.ssh_client.open_sftp,
                                            'self.ssh_client.open_sftp')

        def maybe_fix_mode(local_fn_, remote_fn_):
            """Makes remote file execute for locally executable files"""
            mode = oct(os.stat(local_fn_)[stat.ST_MODE])[-3:]
            if '7' in mode:
                self.log(f"Making {remote_fn_} executable with mode {mode}")
                # use raw run, in case tmux is unavailable
                self._run_raw(f"chmod {mode} {remote_fn_}")

        # augmented SFTP client that can transfer directories, from
        # https://stackoverflow.com/a/19974994/419116
        def _put_dir(source, target):
            """ Uploads the contents of the source directory to the target path."""

            def _safe_mkdir(path, mode=511, ignore_existing=True):
                """ Augments mkdir by adding an option to not fail if the folder exists  asdf asdf asdf as"""
                try:
                    self.sftp.mkdir(path, mode)
                except IOError:
                    if ignore_existing:
                        pass
                    else:
                        raise

            assert os.path.isdir(source)
            _safe_mkdir(target)

            for item in os.listdir(source):
                if os.path.isfile(os.path.join(source, item)):
                    self.sftp.put(os.path.join(source, item), os.path.join(target, item))
                    maybe_fix_mode(os.path.join(source, item), os.path.join(target, item))
                else:
                    _safe_mkdir(f'{target}/{item}')
                    _put_dir(f'{source}/{item}', f'{target}/{item}')

        if not remote_fn:
            remote_fn = os.path.basename(local_fn)

        remote_fn = remote_fn.replace('~', self.home_dir)

        if '/' in remote_fn:
            remote_dir = os.path.dirname(remote_fn)
            assert self.exists(remote_dir), f"Remote dir {remote_dir} doesn't exist"
        if dont_overwrite and self.exists(remote_fn):
            self.log("Remote file %s exists, skipping" % (remote_fn,))
            return

        assert os.path.exists(local_fn), f"{local_fn} not found"
        if os.path.isdir(local_fn):
            self.log('[put dir] ' + local_fn + ' to ' + remote_fn)
            _put_dir(local_fn, remote_fn)
        else:
            assert os.path.isfile(local_fn), "%s is not a file" % (local_fn,)
            # this crashes with IOError when upload failed
            if self.exists(remote_fn) and self.isdir(remote_fn):
                remote_fn = remote_fn + '/' + os.path.basename(local_fn)
            self.log('[upload] ' + local_fn + ' to ' + remote_fn)
            self.sftp.put(localpath=local_fn, remotepath=remote_fn)
            maybe_fix_mode(local_fn, remote_fn)
        if show_info:
            self.log(f'upload {local_fn} to {remote_fn} : done', level='INFO')

    def download(self, remote_fn, local_fn=''):
        self.log("downloading %s" % remote_fn, level='TRACE')
        # sometimes open_sftp fails with Administratively prohibited, do retries
        # root cause could be too many SSH connections being open
        # https://unix.stackexchange.com/questions/14160/ssh-tunneling-error-channel-1-open-failed-administratively-prohibited-open
        if not self.sftp:
            self.sftp = u.call_with_retries(self.ssh_client.open_sftp,
                                            'self.ssh_client.open_sftp')
        if not local_fn:
            local_fn = os.path.basename(remote_fn)

        remote_fn = remote_fn.replace('~', self.home_dir)
        try:
            self.sftp.get(remote_fn, local_fn)
            self.log("downloading %s to %s" % (remote_fn, local_fn), level='TRACE')
            return True
        except Exception as e:
            self.log(f"downloaded {remote_fn} failed with {e.__class__}({e})")
            return False

    def isdir(self, remote_fn):
        stdout, stderr = self._run_raw('ls -ld ' + remote_fn)
        if stdout.startswith('d'):
            self.log(f"[isdir] remote_fn is dir, stdout: {stdout}")
        else:
            self.log(f"[isdir] remote_fn is not dir, stdout: {stdout}")
        return stdout.startswith('d')

    def file_write(self, *args, **kwargs):
        return self.write(*args, **kwargs)

    def exists(self, remote_fn):
        stdout, stderr = self._run_raw('stat ' + remote_fn, ignore_errors=True)
        self.log("exists stdout: [%s]" % stdout, level='TRACE')
        self.log("exists stderr: [%s]" % stderr, level='TRACE')
        """
        the old way should use this function
        return 'No such file or directory' not in stdout
        """

        return 'No such file or directory' not in stderr and 'No such file or directory' not in stdout

    def write(self, remote_fn, contents):
        tmp_fn = self.local_scratch + '/' + str(util.now_micros())
        open(tmp_fn, 'w').write(contents)
        self.upload(tmp_fn, remote_fn, show_info=False)

    def read(self, remote_fn):
        tmp_fn = self.local_scratch + '/' + str(util.now_micros())
        if self.download(remote_fn, tmp_fn):
            context = open(tmp_fn).read()
            # print(f'{context}')
            return context
        else:
            return ''

    @property
    def output(self) -> str:
        self.log(f'_out_fn {self._out_fn}', level='TRACE')
        last_fn = self._out_fn
        return self.read(last_fn)

    def run(self, cmd, sudo=False, non_blocking=False, ignore_errors=False,
            max_wait_sec=365 * 24 * 3600,
            check_interval=1.0, show_realtime=False, show=False):

        if sudo:
            cmd = f"sudo bash -c '{cmd}'"

        # TODO(y): make _run_with_output_on_failure default, and delete this
        # if util.is_set('NCLUSTER_RUN_WITH_OUTPUT_ON_FAILURE'):
        # experimental version that captures output and prints it on failure
        # redirection things break bash commands, so
        # don't redirect on bash commands like source
        # TODO(y): remove this, put in this filtering becase I thought it broke
        # source activate, but now it seems it doesn't
        if not util.is_bash_builtin(cmd) or True:
            return self._run_with_output_on_failure(cmd, non_blocking,
                                                    ignore_errors,
                                                    max_wait_sec,
                                                    show_realtime=show_realtime,
                                                    show=show)
        else:
            self.log("Found bash built-in, using regular run")

        if not self._can_run:
            assert False, "Using .run before initialization finished"

        if '\n' in cmd:
            cmds = cmd.split('\n')
            self.log(
                f"Running {len(cmds)} commands at once, returning status of last")
            status = -1
            for subcmd in cmds:
                status = self.run(subcmd)
                self.last_status = status
            return status

        cmd = cmd.strip()
        if cmd.startswith('#'):  # ignore empty/commented out lines
            return -1
        self.run_counter += 1
        self.log("[tmux run]# %s", cmd)

        _cmd = cmd
        _cmd_fn = f'{self.remote_scratch}/{self.run_counter}.cmd'
        self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'

        cmd = util.shell_strip_comment(cmd)
        aliyun_backend.check_cmd(cmd)

        # modify command to dump shell success status into file
        self.file_write(_cmd_fn, cmd + '\n')
        modified_cmd = f'{cmd}; echo $? > {self._status_fn}'
        modified_cmd = shlex.quote(modified_cmd)

        tmux_window = self.tmux_session + ':' + str(self.tmux_window_id)
        tmux_cmd = f'tmux send-keys -t {tmux_window} {modified_cmd} Enter'
        self.log(f'tmux_cmd: {tmux_cmd}')
        self._run_raw(tmux_cmd, ignore_errors=ignore_errors)
        if non_blocking:
            return 0

        if not self._wait_for_file(self._status_fn, max_wait_sec=2, check_interval=check_interval):
            self.log(f"Retrying waiting for {self._status_fn}")
        while not self.exists(self._status_fn):
            self.log(f"Still waiting for {cmd}")
            self._wait_for_file(self._status_fn, max_wait_sec=10, check_interval=check_interval)
        contents = self.read(self._status_fn)

        # if empty wait a bit to allow for race condition
        if len(contents) == 0:
            time.sleep(check_interval)
            contents = self.read(self._status_fn)
        status = int(contents.strip())
        self.last_status = status

        if status != 0:
            if not ignore_errors:
                raise RuntimeError(f"Command {cmd} returned status {status}")
            else:
                self.log(f"Warning: command {cmd} returned status {status}")

        return status

    def join(self, ignore_errors=False):
        """Waits until last executed command completed."""
        assert self._status_fn, "Asked to join a task which hasn't had any commands executed on it"
        check_interval = 0.2
        status_fn = self._status_fn
        if not self.wait_for_file(status_fn, max_wait_sec=30):
            self.log(f"Retrying waiting for {status_fn}")
        while not self.exists(status_fn):
            self.log(f"Still waiting for {self._cmd}")
            self.wait_for_file(status_fn, max_wait_sec=30)
        contents = self.read(status_fn)

        # if empty wait a bit to allow for race condition
        if len(contents) == 0:
            time.sleep(check_interval)
            contents = self.read(status_fn)
        status = int(contents.strip())
        self.last_status = status

        if status != 0:
            extra_msg = '(ignoring error)' if ignore_errors else '(failing)'
            if util.is_set('NCLUSTER_RUN_WITH_OUTPUT_ON_FAILURE') or True:
                self.log(
                    f"Start failing output {extra_msg}: \n{'*' * 80}\n\n '{self.read(self._out_fn)}'")
                self.log(f"\n{'*' * 80}\nEnd failing output")
            if not ignore_errors:
                raise RuntimeError(f"Command {self._cmd} returned status {status}")
            else:
                self.log(f"Warning: command {self._cmd} returned status {status}")

        return status

    def _run_raw(self, cmd: str, ignore_errors=False) -> Tuple[str, str]:
        """Runs given cmd in the task using current SSH session, returns
    stdout/stderr as strings. Because it blocks until cmd is done, use it for
    short cmds. Silently ignores failing commands.

    This is a barebones method to be used during initialization that have
    minimal dependencies (no tmux)
    """
        #    self._log("run_ssh: %s"%(cmd,))

        stdin, stdout, stderr = u.call_with_retries(self.ssh_client.exec_command,
                                                    command=cmd, get_pty=True)
        stdout_str = stdout.read().decode()
        stderr_str = stderr.read().decode()
        if stdout.channel.recv_exit_status() != 0:
            if not ignore_errors:
                print("failing stdout: " + stdout_str)
                print("failing stderr: " + stderr_str)
                self.log(f"command ({cmd}) failed with --->")
                self.log("failing stdout: " + stdout_str)
                self.log("failing stderr: " + stderr_str)
                assert False, "_run_raw failed (see logs for error)"

        return stdout_str, stderr_str

    def _run_with_output_on_failure(self, cmd, non_blocking=False,
                                    ignore_errors=False,
                                    max_wait_sec=365 * 24 * 3600,
                                    check_interval=0.2,
                                    show_realtime=False,
                                    show=False) -> str:
        """Experimental version of run propagates error messages to client. This command will be default "run" eventually"""

        cmd = cmd.strip()
        if cmd.startswith('#'):  # ignore empty/commented out lines
            return ''
        self.run_counter += 1
        self.log("[bash run] %s", cmd)

        _cmd = cmd
        _cmd_fn = f'{self.remote_scratch}/{self.run_counter}.cmd'
        self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'
        self._out_fn = f'{self.remote_scratch}/{self.run_counter}.out'
        self._err_fn = f'{self.remote_scratch}/{self.run_counter}.err'

        cmd = util.shell_strip_comment(cmd)
        # https://www.gnu.org/software/bash/manual/html_node/Command-Grouping.html

        aliyun_backend.check_cmd(cmd)
        # modify command to dump shell success status into file

        self.file_write(_cmd_fn, cmd + '\n')

        if show_realtime:
            self.log(
                "This command is running with real-time output. A new bash shell will wrap the command for this run. Some commands, e.g. \'cd xxx\' or \'export xxx=xx\', only take effect within this command.",
                level='WARN')

            install_cmd = shlex.quote('conda install -c eumetsat expect -y')
            tmux_window = self.tmux_session + ':' + str(self.tmux_window_id)
            tmux_cmd = f"tmux send-keys -t {tmux_window} {install_cmd} Enter"
            self._run_raw(tmux_cmd, ignore_errors=True)
            # add "set -o pipefail" to output status
            # Ref: https://stackoverflow.com/questions/6871859/piping-command-output-to-tee-but-also-save-exit-code-of-command
            modified_cmd = f'set -o pipefail && unbuffer bash {_cmd_fn} | tee {self._out_fn}; echo $? > {self._status_fn}'
        else:
            cmd = '{ ' + cmd + '; }'  # wrap in { } so that 'cmd1||cmd2 > ...' works
            modified_cmd = f'{cmd} > >(tee -a {self._out_fn}) 2> >(tee -a {self._out_fn} >&2); echo $? > {self._status_fn}'

        modified_cmd = shlex.quote(modified_cmd)

        start_time = time.time()
        tmux_window = self.tmux_session + ':' + str(self.tmux_window_id)
        tmux_cmd = f"tmux send-keys -t {tmux_window} {modified_cmd} Enter"
        self._run_raw(tmux_cmd, ignore_errors=ignore_errors)
        if non_blocking:
            return '0'

        self.output_buffer_line = []
        if not self._wait_for_file_and_output(self._status_fn, max_wait_sec=60, check_interval=0.5,
                                              show_realtime=show_realtime):
            self.log(f"Retrying waiting for {self._status_fn}", level='DEBUG')
        elapsed_time = time.time() - start_time
        while not self.exists(self._status_fn) and elapsed_time < max_wait_sec:
            self.log(f"Still waiting for {cmd}", level='DEBUG')
            self._wait_for_file_and_output(self._status_fn, max_wait_sec=60, check_interval=0.5,
                                           show_realtime=show_realtime)
            elapsed_time = time.time() - start_time
        status = self.read(self._status_fn)

        # if empty wait a bit to allow for race condition
        if len(status) == 0:
            time.sleep(check_interval)
            status = self.read(self._status_fn)
        status = int(status.strip())
        self.last_status = status

        if status != 0:
            extra_msg = '(ignored)' if ignore_errors else '(not ignored)'
            self.log(
                f"Error output {extra_msg}: \n{'*' * 80}\n\n {self.read(self._out_fn)}", level='ERROR')
            print(f"\n{'*' * 80}\nEnd failing output")
            if not ignore_errors:
                raise RuntimeError(f"Command {cmd} returned status {status}")
            else:
                self.log(f"Warning: command {cmd} returned status {status}")

        output = self.output
        # print(output)
        if show_realtime or show:
            self._print_res_output()
        self.log(f'Run {cmd} : done! Location of output: {self._out_fn}', level='INFO')
        self.log(f'output: {output}')
        return output

    def _wait_for_file_and_output(self, fn: str, max_wait_sec: int = 3600 * 24 * 365,
                                  check_interval: float = 1.0, show_realtime=False) -> bool:
        """
        Waits for file maximum of max_wait_sec. Returns True if file was detected within specified max_wait_sec
        Args:
          fn: filename on task machine
          max_wait_sec: how long to wait in seconds
          check_interval: how often to check in seconds
        Returns:
          False if waiting was was cut short by max_wait_sec limit, True otherwise
        """
        #    print("Waiting for file", fn)
        start_time = time.time()
        while True:
            if show_realtime:
                self._print_res_output()
            if time.time() - start_time > max_wait_sec:
                self.log(f"Timeout exceeded ({max_wait_sec} sec) for {fn}")
                return False
            if not self.exists(fn):
                time.sleep(check_interval)
                continue
            else:
                break
        return True


class Job(backend.Job):
    pass


class Run(backend.Run):
    """Run is a collection of jobs that share state. IE, training run will contain gradient worker job, parameter
  server job, and TensorBoard visualizer job. These jobs will use the same shared directory to store checkpoints and
  event files.
  """
    placement_group: str  # unique identifier to use as placement_group group name
    jobs: List[Job]

    def __init__(self, name='', **kwargs):
        """Creates a run. If install_script is specified, it's used as default
    install_script for all jobs (can be overridden by Job constructor)"""

        assert name, "Must specify name for current run"

        jobs = []
        self.name = name
        self.jobs = jobs
        self.kwargs = kwargs
        self.placement_group = name + '-' + util.random_id()
        util.log(f"Choosing placement_group for run {name} to be {self.placement_group}")

    @property
    def logdir(self):
        # querying logdir has a side-effect of creation, so do it on chief task
        chief_task = ncluster_globals.get_chief(self.name)
        return chief_task.logdir

    # TODO: currently this is synchronous, use non_blocking wrapper like in Job to parallelize methods
    def run(self, *args, **kwargs):
        """Runs command on every job in the run."""

        for job in self.jobs:
            job.run(*args, **kwargs)

    def run_with_output(self, *args, **kwargs):
        """Runs command on every first job in the run, returns stdout."""
        for job in self.jobs:
            job.run_with_output(*args, **kwargs)

    def _run_raw(self, *args, **kwargs):
        """_run_raw on every job in the run."""
        for job in self.jobs:
            job._run_raw(*args, **kwargs)

    def upload(self, *args, **kwargs):
        """Runs command on every job in the run."""
        for job in self.jobs:
            job.upload(*args, **kwargs)

    def make_job(self, name='', **kwargs):
        return make_job(name + '.' + self.name, run_name=self.name, **kwargs)

    def stop(self):
        """Stop the every job"""
        for job in self.jobs:
            job.stop()


def make_task(name: str = '',
              run_name: str = '',
              install_script: str = '',
              public_ip: str = '',
              username: str = '',
              password: str = '',
              eth: int = 0,
              **_kwargs
              ) -> Task:
    task = Task(name=name,
                run_name=run_name,
                install_script=install_script,
                public_ip=public_ip,
                username=username,
                password=password,
                eth=eth)

    return task


def make_job(
        name: str = '',
        run_name: str = '',
        num_tasks: int = 1,
        install_script: str = '',
        tasks_message: list = [],
        ssh_auth_for_task0=True,
        **kwargs
) -> Job:
    """
    Args:
        name: see backend.make_task
        run_name: see backend.make_task
        num_tasks: number of tasks to launch
        install_script:
        tasks_message:the public_ips usernames tasks_messages instance_types of the instances in the tasks
        ssh_auth_for_task0:local can ssh task0 without password
    Returns:
        Job
    """
    assert num_tasks > 0, f"Can't create job with {num_tasks} tasks"

    # dummy tasks for logging
    tasks = [backend.Task(f"task{i}.{name}") for i in range(num_tasks)]

    name = ncluster_globals.auto_assign_job_name_if_needed(name)
    run_name = ncluster_globals.auto_assign_run_name_if_needed(run_name)
    _run = ncluster_globals.create_run_if_needed(run_name, aliyun_backend.make_run)

    job = Job(name=name, tasks=tasks, run_name=run_name, **kwargs)

    exceptions = []

    # make tasks in parallel
    def make_task_fn(i: int):
        try:
            tasks[i] = make_task(f"task{i}.{name}",
                                 run_name=run_name,
                                 install_script=install_script,
                                 public_ip=tasks_message[i][0],
                                 username=tasks_message[i][1],
                                 password=tasks_message[i][2],
                                 **kwargs)
        except Exception as e:
            exceptions.append(e)

    # Creating threads for making tasks
    util.log("Creating threads")
    threads = [threading.Thread(name=f'make_task_{i}',
                                target=make_task_fn, args=[i])
               for i in range(num_tasks)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if exceptions:
        print("Exception are ", exceptions)
        raise exceptions[0]

    job.tasks = tasks

    if ncluster_globals.should_skip_setup():
        ssh_auth_for_task0 = False

    # get task0's public key
    if ssh_auth_for_task0 and len(job.tasks) > 0:
        ssh_cmd = ['FILE=/root/.ssh/id_rsa',
                   'if [ -f "$FILE" ]; then',
                   '  echo "$FILE exist"',
                   'else',
                   '  cat /dev/zero | ssh-keygen -q -N ""',
                   'fi']
        job.tasks[0].run('\n'.join(ssh_cmd))
        id_rsa_pub = job.tasks[0].run('cat ~/.ssh/id_rsa.pub')

        # let task0 can ssh to other tasks without password
        for task in job.tasks:
            # can be a function in class Task -> def add_pub(id_rsa_pub)
            stdout_str = task.run('cat ~/.ssh/authorized_keys')
            if id_rsa_pub not in stdout_str:
                task.run(f'echo \"{id_rsa_pub}\" >> ~/.ssh/authorized_keys')

    return job


def conf_xml(xml_path, name_value):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    for name in name_value:
        for property in root.findall("property"):
            if property.find("name").text == name:
                property.find("value").text = name_value[name]
                break
    tree.write(xml_path)
