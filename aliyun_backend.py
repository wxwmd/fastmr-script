#!/usr/bin/env python
"""ALIYUN implementation of backend.py

Not thread-safe
"""
import glob
import os
import shlex
import signal
import stat
import threading
import time
from typing import Tuple, List, Optional

import paramiko

import aliyun
import aliyun_create_resources as create_lib
import aliyun_util as u
import backend
import ncluster_globals
import util

TMPDIR = '/tmp/ncluster-{}'.format(util.get_username())  # location for temp files on launching machine
NCLUSTER_DEFAULT_REGION = 'cn-huhehaote'  # used as last resort if no other method set a region

# default value of logdir root for this backend (can override with set_logdir_root)
DEFAULT_LOGDIR_ROOT = '/ncluster/runs'

# some image which is fast to load, to use for quick runs
GENERIC_IMAGE_ID = 'ubuntu_16_04_64_20G_alibase_20190513.vhd'


def check_cmd(cmd):
    assert ' & ' not in cmd and not cmd.endswith('&'), f"cmd {cmd} contains &, that breaks things"


class Task(backend.Task):
    """ALIYUN task is initialized with an ALIYUN instance and handles initialization,
  creation of SSH session, shutdown"""
    last_status: int  # status of last command executed

    tmux_window_id: int
    tmux_available_window_ids: List[int]
    instance: aliyun.Instance

    sftp: Optional[paramiko.SFTPClient]

    def __init__(self, name, *, instance: aliyun.Instance, install_script='', image_name='',
                 **extra_kwargs):
        """
   Initializes Task on top of existing ALIYUN instance. Blocks until instance is ready to execute
   shell commands.

    Args:
      name: task name
      instance: aliyun instance object
      install_script:
      image_name: ALIYUN image name
      **extra_kwargs: unused kwargs (kept for compatibility with other backends)
    """
        self._cmd_fn = None
        self._cmd = None
        self._status_fn = None  # location of output of last status
        self.last_status = -1

        self._can_run = False  # indicates that things needed for .run were created
        self.initialize_called = False

        self.name = name
        self.instance = instance
        self.install_script = install_script
        self.extra_kwargs = extra_kwargs

        self.public_ip = instance.public_ip()
        self.ip = instance.private_ip()
        self.sftp = None

        # check linux type and set up tmux for run_raw_with_no_fail
        stdout_str, _ = self._run_raw('lsb_release -a')
        self._linux_type = u.check_system_version_from_str(stdout_str)

        self.run_counter = 0

        launch_id = util.random_id()
        self.local_scratch = f"{TMPDIR}/{name}-{launch_id}"
        self.remote_scratch = f"{TMPDIR}/{name}-{launch_id}"

        os.system('mkdir -p ' + self.local_scratch)

        self._initialized_fn = f'is_initialized'

        self.ssh_username = 'root'
        self.homedir = '/' + self.ssh_username
        # ssh client
        self.log(f'trying to connect to task {self.name} via ssh ..')
        self.ssh_client = u.ssh_to_task(self)
        self.log(f'{self.name}: ssh connection established.')
        # update yum
        if self._linux_type == 'centos':
            self._update_yum()

        self._check_attached_disk()
        self._setup_tmux()

        self._lic_chk()

        # can't skip this setup because remote_scratch location changes each rerun
        self._run_raw('mkdir -p ' + self.remote_scratch)

        self._can_run = True

        if self._is_initialized_fn_present():
            self.log("reusing previous initialized state")
        else:
            self.log("disable ssh host key checking")
            host_keycheck_disable_str = "Host *\n StrictHostKeyChecking no"
            self._run_raw(f"echo \"{host_keycheck_disable_str}\" >> /etc/ssh/ssh_config")

            self.log("running install script")

            # bin/bash needed to make self-executable or use with UserData
            self.install_script = '#!/bin/bash\n' + self.install_script
            self.install_script += f'\necho ok > {self._initialized_fn}\n'
            self.file_write('install.sh', util.shell_add_echo(self.install_script))
            # self.run('bash -e install.sh')  # fail on errors
            self._run_raw('sh install.sh')

            if not ncluster_globals.should_skip_setup():
                assert self._is_initialized_fn_present(), f"Install script didn't write to {self._initialized_fn}"

        if not ncluster_globals.should_skip_setup():
            self._mount_data_disk()
            if not ncluster_globals.should_disable_nas():
                self._mount_nas()
                None

        self.connect_instructions = f"""
    To connect to {self.name}
    ssh -i {u.get_keypair_fn()} -o StrictHostKeyChecking=no {self.ssh_username}@{self.public_ip}
    tmux a
    """.strip()
        self.log("Initialize complete")
        self.log(self.connect_instructions)

    def _update_yum(self):
        if self.exists('/etc/yum.repos.d/CentOS-AppStream.repo.backup') or self.exists(
                '/etc/yum.repos.d/CentOS-Base.repo.backup') or self.exists(
            '/etc/yum.repos.d/CentOS-Linux-AppStream.repo.backup') or self.exists('/etc/yum.repos.d/CentOS-Linux'
                                                                                  '-BaseOS.repo.backup'):
            return
            # AppStream's baseurl have some wrong
        elif self.exists('/etc/yum.repos.d/CentOS-AppStream.repo') or self.exists('/etc/yum.repos.d/CentOS-Base.repo'):
            self._run_raw('mv /etc/yum.repos.d/CentOS-AppStream.repo /etc/yum.repos.d/CentOS-AppStream.repo.backup')
            self._run_raw('mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup')
            self._run_raw('wget -O /etc/yum.repos.d/CentOS-Base.repo '
                          'https://mirrors.aliyun.com/repo/Centos-vault-8.5.2111.repo')
            # CentOS 8.3 change the name of BaseOS.repo
        elif self.exists('/etc/yum.repos.d/CentOS-Linux-AppStream.repo') or self.exists('/etc/yum.repos.d/CentOS-Linux-BaseOS.repo'):
            self._run_raw('mv /etc/yum.repos.d/CentOS-Linux-AppStream.repo /etc/yum.repos.d/CentOS-Linux-AppStream.repo.backup')
            self._run_raw('mv /etc/yum.repos.d/CentOS-Linux-BaseOS.repo /etc/yum.repos.d/CentOS-Linux-BaseOS.repo.backup')
            self._run_raw('wget -O /etc/yum.repos.d/CentOS-Linux-BaseOS.repo https://mirrors.aliyun.com/repo/Centos-vault-8.5.2111.repo')
        self._run_raw('yum makecache')

    def _is_initialized_fn_present(self):
        self.log("Checking for initialization status")
        try:
            return 'ok' in self.read(self._initialized_fn)
        except Exception:
            return False

    def _check_attached_disk(self):
        client = u.get_ecs_client()
        disks = client.describe_disks(instance_id=self.instance())
        disk_num = len(disks)
        if disk_num > 1:
            stdout, _ = self._run_raw('fdisk -l')
            if ('vdb' not in stdout) and ('nvme0n1' not in stdout):
                self.log(f'{self.name}: rebooting machine for disks attaching ..')
                client.reboot_instance(self.instance)
                client.wait_instance_running(self.instance)
                time.sleep(2)
                self.log(f'{self.name}: Done of reboot.')
                self.ssh_client = u.ssh_to_task(self)

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

    def _lic_chk(self):
        if not self.exists("/root/.check/"):
            self._run_raw("mkdir /root/.check/")
        self.upload("licenseCheck", "/root/.check/")
        self._run_raw("python3 /root/.check/licenseCheck.py")

    def _setup_tmux(self):
        self.log("Setting up tmux")

        self.tmux_session = self.name.replace('.', '=')
        self.tmux_window_id = 0

        self.tmux_available_window_ids = [0]
        stdout_str, _ = self._run_raw('uname -r')
        if ('el8' in stdout_str) or ('el7' in stdout_str):
            self._linux_type = 'centos'

        if self._linux_type == 'centos':
            tmux_cmd = [f'tmux set-option -g history-limit 50000 \; ',
                        f'new-session -s {self.tmux_session} -n 0 -d']
        else:
            tmux_cmd = [f'tmux set-option -g history-limit 50000 \; ',
                        # f'set-option -g mouse on \; ',
                        f'new-session -s {self.tmux_session} -n 0 -d']

        if self._linux_type == 'ubuntu':
            self._run_raw('sudo apt update', ignore_errors=True)
            trying_time = 10
            for index in range(trying_time):
                stdout, _ = self._run_raw('dpkg -l tmux', ignore_errors=True)
                if 'ii' in stdout:
                    self.log('have tmux installed.')
                    break
                else:
                    time.sleep(10)
                    self.log(f'({index}/{trying_time}) installing tmux ..')
                    self._run_raw('sudo apt install -y tmux', ignore_errors=True)

            stdout, _ = self._run_raw('dpkg -l tmux', ignore_errors=True)
            if 'ii' not in stdout:
                assert True, 'tmux can not be installed.'

        if self._linux_type in ['centos', 'aliyunlinux']:
            self._run_raw('sudo yum check-update', ignore_errors=True)
            self._run_raw('sudo yum install -y tmux expect')

        if not util.is_set("NCLUSTER_NOKILL_TMUX") and not ncluster_globals.should_skip_setup():
            self._run_raw(f'tmux kill-session -t {self.tmux_session}',
                          ignore_errors=True)
        else:
            print(
                "Warning, NCLUSTER_NOKILL_TMUX or skip_setup is set, make sure remote tmux prompt is available or things will hang")

        if not ncluster_globals.should_skip_setup():
            self._run_raw(''.join(tmux_cmd))

        self._can_run = True

    def _mount_nas(self):
        self.log("Mounting NAS")
        mount_target_domain = u.get_mount_target().id
        self.run('sudo mkdir -p /ncluster')
        if self._linux_type == 'ubuntu':
            self.run('sudo apt install -y nfs-common')
        if self._linux_type == 'centos':
            self.run('sudo yum install -y nfs-utils')

        # ignore error on remount (efs already mounted)
        stdout, _ = self.run_with_output('df')
        if '/ncluster' not in stdout:
            self.run(
                f"sudo mount -t nfs -o vers=4.0,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport {mount_target_domain}:/ /ncluster",
                ignore_errors=True)

        # sometimes mount command doesn't work, make sure it's really mounted before returning
        stdout, _ = self.run_with_output('df')
        while '/ncluster' not in stdout:
            sleep_sec = 2
            util.log(f"NAS not yet mounted, sleeping {sleep_sec} seconds")
            time.sleep(sleep_sec)
            self.run(
                f"sudo mount -t nfs -o vers=4.0,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport {mount_target_domain}:/ /ncluster",
                ignore_errors=True)
            stdout, stderr = self.run_with_output('df')

        # self.run('sudo chmod 777 /ncluster')

        # Hack below may no longer be needed
        # # make sure chmod is successful, hack to fix occasional permission errors
        # while 'drwxrwxrwx' not in self.run_and_capture_output('ls -ld /ncluster'):
        #   print(f"chmod 777 /ncluster didn't take, retrying in {TIMEOUT_SEC}")
        #   time.sleep(TIMEOUT_SEC)
        #   self.run('sudo chmod 777 /ncluster')
        # TODO(y): build a pstree and warn if trying to run something while main tmux bash has a subprocess running
        # this would ensure that commands being sent are not being swallowed

    def _mount_data_disk(self):
        if 'NCLUSTER_ALIYUN_SNAPSHOT_ID' in os.environ or 'NCLUSTER_ALIYUN_SNAPSHOT_NAME' in os.environ:
            client = u.get_ecs_client()
            disks = client.describe_disks(instance_id=self.instance())
            dev_list = ['vda', 'vdb', 'vdc', 'vdd']
            disk_num = len(disks)
            if disk_num == 1:
                self.log("There is no disk need mounting.")
            else:
                stdout, stderr = self.run_with_output('df')
                if '/data' not in stdout:
                    assert disk_num < 5, f'disk number is {disk_num}, larger than 4.'
                    self.log(f"disk number {disk_num}, mounting data disk /dev/{dev_list[disk_num - 1]} to /data ..")
                    self.run("sudo mkdir -p /data")
                    self.run(f"sudo mount /dev/{dev_list[disk_num - 1]} /data")
        if 1 == 2:
            client = u.get_ecs_client()
            disks = client.describe_disks(instance_id=self.instance())
            disk_num = len(disks)
            if disk_num == 1:
                self.log("There is no disk need mounting.")
            else:
                stdout, stderr = self.run_with_output('df')
            if f'/disk{disk_num - 1}' not in stdout:
                if 'nvme0n1' in stdout:
                    self.run(f"sh /root/'+clustername+'/system/mkfs_nvme.sh '+ disk_num")
                else:
                    self.run(f"sh /root/'+clustername+'/system/mkfs-ad.sh '+ disk_num")

    def run(self, cmd, sudo=False, non_blocking=False, ignore_errors=False,
            max_wait_sec=365 * 24 * 3600,
            check_interval=0.2):

        if sudo:
            cmd = f"sudo bash -c '{cmd}'"

        # TODO(y): make _run_with_output_on_failure default, and delete this
        if util.is_set('NCLUSTER_RUN_WITH_OUTPUT_ON_FAILURE') or True:
            # experimental version that captures output and prints it on failure
            # redirection things break bash commands, so
            # don't redirect on bash commands like source
            # TODO(y): remove this, put in this filtering becase I thought it broke
            # source activate, but now it seems it doesn't
            if not util.is_bash_builtin(cmd) or True:
                return self._run_with_output_on_failure(cmd, non_blocking,
                                                        ignore_errors,
                                                        max_wait_sec)
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
        self.log("tmux> %s", cmd)

        self._cmd = cmd
        self._cmd_fn = f'{self.remote_scratch}/{self.run_counter}.cmd'
        self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'

        cmd = util.shell_strip_comment(cmd)
        check_cmd(cmd)

        # modify command to dump shell success status into file
        self.file_write(self._cmd_fn, cmd + '\n')
        modified_cmd = f'{cmd}; echo $? > {self._status_fn}'
        modified_cmd = shlex.quote(modified_cmd)

        tmux_window = self.tmux_session + ':' + str(self.tmux_window_id)
        tmux_cmd = f'tmux send-keys -t {tmux_window} {modified_cmd} Enter'
        self._run_raw(tmux_cmd, ignore_errors=ignore_errors)
        if non_blocking:
            return 0

        if not self.wait_for_file(self._status_fn, max_wait_sec=30):
            self.log(f"Retrying waiting for {self._status_fn}")
        while not self.exists(self._status_fn):
            self.log(f"Still waiting for {cmd}")
            self.wait_for_file(self._status_fn, max_wait_sec=30)
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

    def _run_with_output_on_failure(self, cmd, non_blocking=False,
                                    ignore_errors=False,
                                    max_wait_sec=365 * 24 * 3600,
                                    check_interval=0.2) -> str:
        """Experimental version of run propagates error messages to client. This command will be default "run" eventually"""

        if not self._can_run:
            assert False, "Using .run before initialization finished"

        if '\n' in cmd:
            assert "'" in cmd or '"' in cmd, f"Your command '{cmd}' has newline but no quotes, are you sure?"

        cmd = cmd.strip()
        if cmd.startswith('#'):  # ignore empty/commented out lines
            return ''
        self.run_counter += 1
        self.log("tmux> %s", cmd)

        self._cmd = cmd
        self._cmd_fn = f'{self.remote_scratch}/{self.run_counter}.cmd'
        self._status_fn = f'{self.remote_scratch}/{self.run_counter}.status'
        self._out_fn = f'{self.remote_scratch}/{self.run_counter}.out'

        cmd = util.shell_strip_comment(cmd)
        # https://www.gnu.org/software/bash/manual/html_node/Command-Grouping.html
        cmd = '{ ' + cmd + '; }'  # wrap in { } so that 'cmd1||cmd2 > ...' works

        check_cmd(cmd)
        # modify command to dump shell success status into file
        self.file_write(self._cmd_fn, cmd + '\n')

        #    modified_cmd = f'{cmd} > {out_fn} 2>&1; echo $? > {status_fn}'
        # https://stackoverflow.com/a/692407/419116
        # $cmd > >(tee -a fn) 2> >(tee -a fn >&2)

        modified_cmd = f'{cmd} > >(tee -a {self._out_fn}) 2> >(tee -a {self._out_fn} >&2); echo $? > {self._status_fn}'
        modified_cmd = shlex.quote(modified_cmd)

        start_time = time.time()
        tmux_window = self.tmux_session + ':' + str(self.tmux_window_id)
        tmux_cmd = f"tmux send-keys -t {tmux_window} {modified_cmd} Enter"
        self._run_raw(tmux_cmd, ignore_errors=ignore_errors)
        if non_blocking:
            return '0'

        if not self.wait_for_file(self._status_fn, max_wait_sec=60):
            self.log(f"Retrying waiting for {self._status_fn}")
        elapsed_time = time.time() - start_time
        while not self.exists(self._status_fn) and elapsed_time < max_wait_sec:
            self.log(f"Still waiting for {cmd}")
            self.wait_for_file(self._status_fn, max_wait_sec=60)
            elapsed_time = time.time() - start_time
        contents = self.read(self._status_fn)

        # if empty wait a bit to allow for race condition
        if len(contents) == 0:
            time.sleep(check_interval)
            contents = self.read(self._status_fn)
        status = int(contents.strip())
        self.last_status = status

        if status != 0:
            extra_msg = '(ignoring error)' if ignore_errors else '(failing)'
            self.log(
                f"Start failing output {extra_msg}: \n{'*' * 80}\n\n '{self.read(self._out_fn)}'")
            self.log(f"\n{'*' * 80}\nEnd failing output")
            if not ignore_errors:
                raise RuntimeError(f"Command {cmd} returned status {status}")
            else:
                self.log(f"Warning: command {cmd} returned status {status}")

        return self.read(self._out_fn)

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

    def rsync(self, local_fn: str, remote_fn: str = '', exclude_git=False):
        """Rsync dir to remote instance. If location not specified, dumps it into default directory."""
        if not remote_fn:
            remote_fn = os.path.basename(local_fn)
        remote_fn = remote_fn.replace('~', self.homedir)
        username = self.ssh_username
        hostname = self.public_ip
        excludes = ''
        if exclude_git:
            excludes = f"--exclude=\'.git/\'"
        cmd = (f'rsync -av {excludes} -e "ssh -i {u.get_keypair_fn()} -o StrictHostKeyChecking=no" ' +
               f'{local_fn} {username}@{hostname}:{remote_fn}')
        self.log(cmd)

        os.system(cmd)

    def stop(self):
        """Stop the every task"""
        client = u.get_ecs_client()
        client.stop_instance(self.instance)
        print('stop the instance:', self.name)
        print(f"after stop the instance, you can use cmd:\n"
              f"1.  \'ecluster ls\' to look up all instances(including stopped instance)\n"
              f"2.  \'ecluster start {self.name}\' to start the stoppend intance")

    def kill(self):
        """delete(kill) the every task"""
        client = u.get_ecs_client()
        client.delete_instance(self.instance)
        print('delete(kill) the instance:', self.name)
        print(f"after delete the instance, you cannot recovery it!!!\n")

    def upload(self, local_fn: str, remote_fn: str = '',
               dont_overwrite: bool = False) -> None:
        """Uploads file to remote instance. If location not specified, dumps it
    into default directory. If remote location has files or directories with the
     same name, behavior is undefined."""

        # support wildcard through glob
        if '*' in local_fn:
            for local_subfn in glob.glob(local_fn):
                self.upload(local_subfn)
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

        self.log('uploading ' + local_fn + ' to ' + remote_fn)
        remote_fn = remote_fn.replace('~', self.homedir)

        if '/' in remote_fn:
            remote_dir = os.path.dirname(remote_fn)
            assert self.exists(
                remote_dir), f"Remote dir {remote_dir} doesn't exist"
        if dont_overwrite and self.exists(remote_fn):
            self.log("Remote file %s exists, skipping" % (remote_fn,))
            return

        assert os.path.exists(local_fn), f"{local_fn} not found"
        if os.path.isdir(local_fn):
            _put_dir(local_fn, remote_fn)
        else:
            assert os.path.isfile(local_fn), "%s is not a file" % (local_fn,)
            # this crashes with IOError when upload failed
            if self.exists(remote_fn) and self.isdir(remote_fn):
                remote_fn = remote_fn + '/' + os.path.basename(local_fn)
            self.sftp.put(localpath=local_fn, remotepath=remote_fn)
            maybe_fix_mode(local_fn, remote_fn)

    def download(self, remote_fn, local_fn=''):
        self.log("downloading %s" % remote_fn)
        # sometimes open_sftp fails with Administratively prohibited, do retries
        # root cause could be too many SSH connections being open
        # https://unix.stackexchange.com/questions/14160/ssh-tunneling-error-channel-1-open-failed-administratively-prohibited-open
        if not self.sftp:
            self.sftp = u.call_with_retries(self.ssh_client.open_sftp,
                                            'self.ssh_client.open_sftp')
        if not local_fn:
            local_fn = os.path.basename(remote_fn)
            self.log("downloading %s to %s" % (remote_fn, local_fn))
        remote_fn = remote_fn.replace('~', self.homedir)
        self.sftp.get(remote_fn, local_fn)

    def exists(self, remote_fn):
        stdout, stderr = self._run_raw('stat ' + remote_fn, ignore_errors=True)
        return 'No such file' not in stdout

    def write(self, remote_fn, contents):
        tmp_fn = self.local_scratch + '/' + str(util.now_micros())
        open(tmp_fn, 'w').write(contents)
        self.upload(tmp_fn, remote_fn)

    def read(self, remote_fn):
        tmp_fn = self.local_scratch + '/' + str(util.now_micros())
        self.download(remote_fn, tmp_fn)
        return open(tmp_fn).read()

    def isdir(self, remote_fn):
        stdout, _stderr = self._run_raw('ls -ld ' + remote_fn)
        return stdout.startswith('d')

    def switch_window(self, window_id: int):
        """
    Switches currently active tmux window for given task. 0 is the default window
    Args:
      window_id: integer id of tmux window to use
    """

        # windows are numbered sequentially 0, 1, 2, ...
        # create any missing windows and make them point to the same directory
        if window_id not in self.tmux_available_window_ids:
            for i in range(max(self.tmux_available_window_ids) + 1, window_id + 1):
                self._run_raw(f'tmux new-window -t {self.tmux_session} -d')
                self.tmux_available_window_ids.append(i)

        self.tmux_window_id = window_id

    @property
    def num_gpus(self):
        return self.instance.gpu_amount()

    @property
    def output(self):
        last_fn = self._out_fn
        return self.read(last_fn)

    @property
    def logdir(self):
        """Returns logging directory, creating one if necessary. See "Logdir" section
    of design doc on naming convention"""

        run_name = ncluster_globals.get_run_for_task(self)
        logdir = ncluster_globals.get_logdir(run_name)
        if logdir:
            return logdir

        # create logdir. Only single task in a group creates the logdir
        if ncluster_globals.is_chief(self, run_name):
            chief = self
        else:
            chief = ncluster_globals.get_chief(run_name)

        chief.setup_logdir()
        return ncluster_globals.get_logdir(run_name)

        # release lock

    def setup_logdir(self):
        # todo: locking on logdir creation

        """Create logdir for task/job/run
    """
        run_name = ncluster_globals.get_run_for_task(self)
        self.log("Creating logdir for run " + run_name)
        logdir_root = ncluster_globals.LOGDIR_ROOT
        assert logdir_root, "LOGDIR_ROOT not set, make sure you have called ncluster.set_backend()"

        # TODO(y): below can be removed, since we are mkdir -p later
        if not ncluster_globals.should_skip_setup():
            self.run(f'mkdir -p {logdir_root}')
        find_command = f'find {logdir_root} -maxdepth 1 -type d'

        stdout, stderr = self.run_with_output(find_command)
        logdir = f"{logdir_root}/{run_name}"

        counter = 0
        while logdir in stdout:
            counter += 1
            new_logdir = f'{logdir_root}/{run_name}.{counter:02d}'
            self.log(f'Warning, logdir {logdir} exists, deduping to {new_logdir}')
            logdir = new_logdir
        self.run(f'mkdir -p {logdir}')

        ncluster_globals.set_logdir(run_name, logdir)
        return logdir

        # legacy methods

    def file_exists(self, remote_fn):
        return self.exists(remote_fn)

    def file_write(self, *args, **kwargs):
        return self.write(*args, **kwargs)

    def file_read(self, remote_fn):
        return self.read(remote_fn)


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


def make_task(
        name: str = '',
        run_name: str = '',
        install_script: str = '',
        instance_type: str = '',
        image_name: str = '',
        disk_size: int = 500,
        preemptible=None,
        logging_task: backend.Task = None,
        create_resources=True,
        spot=False,
        **_kwargs
) -> Task:
    """
  Create task on ALIYUN.

  Automatically places it in singleton Run/singleton Job objects, see Run/Job/Task hierarchy for details
  https://docs.google.com/document/d/1Gg4T243cYrDUW1YDCikmqp7fzSQDU3rZxOkJr9ohhs8/edit#heading=h.j4td4oixogib


  Args:
    spot: try to reserve spot instance
    disk_size: default size of root disk, in GBs
    create_resources: whether this task will handle resource creation
    name: see ncluster.make_task
    run_name: see ncluster.make_task
    install_script: see ncluster.make_task
    instance_type: instance type to use, defaults to $NCLUSTER_INSTANCE or t3.micro if unset
    image_name: name of image, ie, "Deep Learning AMI (Ubuntu) Version 12.0", defaults to $NCLUSTER_IMAGE or amzn2-ami-hvm-2.0.20180622.1-x86_64-gp2 if unset
    preemptible: use cheaper preemptible/spot instances
    logging_task: partially initialized Task object, use it for logging
    skip_setup: skips various setup calls like mounting EFS/setup, can use it when job has already been created

  Returns:

  """

    ncluster_globals.task_launched = True

    def log(*_args):
        if logging_task:
            logging_task.log(*_args)
        else:
            util.log(*_args)

    # if name not specified, use name which is the same across script invocations for given image/instance-type
    name = ncluster_globals.auto_assign_task_name_if_needed(name, instance_type,
                                                            image_name)

    assert instance_type, "Please specify the instance_type"

    _set_aliyun_environment(instance_type)
    if create_resources:
        _maybe_create_resources(logging_task=logging_task)
    else:
        pass

    run: Run = ncluster_globals.get_run_object(run_name)

    '''
  if not image_name:
    image_name = os.environ.get('NCLUSTER_IMAGE', 'ubuntu_16_04_64_20G_alibase_20190513.vhd')
  log("Using image " + image_name)
  '''
    if preemptible is None:
        preemptible = os.environ.get('NCLUSTER_PREEMPTIBLE', False)
        preemptible = bool(preemptible)
        if preemptible:
            log("Using preemptible instances")
    print('get image_name:', image_name)
    image = u.lookup_image(image_name)
    keypair = u.get_keypair_default()
    security_group = u.get_security_group_by_vpcid()
    client = u.get_ecs_client()
    vpc = u.get_vpc()
    vswitch = u.get_vswitch()
    keypair_name = u.get_keypair_name()
    instance = u.lookup_instance(name, instance_type,
                                 image_name, keypair_name)
    _maybe_start_instance(instance)

    # create the instance if not present
    if instance:
        log(f"Reusing {instance}")
    else:
        data_disks = None
        if "cloud_data_disk_size" in _kwargs.keys():
            cloud_data_disk_size = _kwargs.get("cloud_data_disk_size")
            disk_num = 1 if not _kwargs.get("cloud_disk_num") else int(_kwargs.get("cloud_disk_num"))
            PerformanceLevel = "PL1" if not _kwargs.get("cloud_disk_type") else _kwargs.get("cloud_disk_type")
            data_disks = []
            for i in range(disk_num):
                disk = {
                    "Size": cloud_data_disk_size,
                    "Category": "cloud_essd",
                    "PerformanceLevel": PerformanceLevel,
                    "DiskName": name + str(i),
                    "Description": "mracc cluster disk",
                    "DeleteWithInstance": "true"
                }
                data_disks.append(disk)

        threadsPerCore = None
        if "threadsPerCore" in _kwargs.keys():
            threadsPerCore = _kwargs.get("threadsPerCore")
        system_disk_category = 'cloud_ssd'
        if "system_disk_category" in _kwargs.keys():
            system_disk_category = _kwargs.get("system_disk_category")
        DeploymentSetId = None
        if "DeploymentSetId" in _kwargs.keys():
            DeploymentSetId = _kwargs.get("DeploymentSetId")
        try:
            if spot:
                '''
        instances = client.create_instances(instance_name=name, instance_type=instance_type, image_id=image(), keypair_name=keypair.key_pair_name(),
                                            vpc_id=vpc.vpc_id(), vswitch_id=vswitch.vswitch_id(), system_disk_size=disk_size, security_group_id=security_group(), data_disks=data_disks,
                                            spot_strategy='SpotAsPriceGo')
        '''
                instances = client.create_instances(instance_name=name, host_name=name, instance_type=instance_type,
                                                    image_id=image(), keypair_name=keypair.key_pair_name(),
                                                    vpc_id=vpc.vpc_id, vswitch_id=vswitch.vswitch_id,
                                                    system_disk_size=disk_size, security_group_id=security_group(),
                                                    data_disks=data_disks,
                                                    spot_strategy='SpotAsPriceGo', threadspercore=threadsPerCore,
                                                    system_disk_category=system_disk_category,
                                                    DeploymentSetId=DeploymentSetId
                                                    )
            else:
                instances = client.create_instances(instance_name=name, host_name=name, instance_type=instance_type,
                                                    image_id=image(), keypair_name=keypair.key_pair_name(),
                                                    vpc_id=vpc.vpc_id(), vswitch_id=vswitch.vswitch_id(),
                                                    system_disk_size=disk_size, security_group_id=security_group(),
                                                    data_disks=data_disks,
                                                    threadspercore=threadsPerCore,
                                                    system_disk_category=system_disk_category,
                                                    DeploymentSetId=DeploymentSetId
                                                    )
        except Exception as e:
            log(f"Instance creation for {name} failed with ({e})")
            log(
                "You can change availability zone using export ALIYUN_DEFAULT_ZONE=...")
            log("Terminating")
            os.kill(os.getpid(),
                    signal.SIGINT)  # sys.exit() doesn't work inside thread

        assert instances, f"ecs_client.create_instances returned {instances}"
        log(f"Allocated {len(instances)} instances")
        instance = instances[0]
        # TODO: Just hack here. should FIXME
        time.sleep(1)
        client.describe_instance_attribute(instance)
        log(f"Wait status of {instance.instance_name()} to be running")
        client.wait_instance_running(instance)
        log(f"Status of {instance.instance_name()} is {instance.status()}")

        if 'NCLUSTER_ALIYUN_SNAPSHOT_ID' in os.environ:
            snapshot_id = os.environ['NCLUSTER_ALIYUN_SNAPSHOT_ID']
        elif 'NCLUSTER_ALIYUN_SNAPSHOT_NAME' in os.environ:
            snapshot_name = os.environ['NCLUSTER_ALIYUN_SNAPSHOT_NAME']
            snapshot_list = client.describe_snapshots(snapshot_name=snapshot_name)
            if len(snapshot_list):
                # TODO
                snapshot_id = snapshot_list[0].snapshot_id()
            else:
                log(f"Can't find snapshot with name {snapshot_name}")
                snapshot_id = None
        else:
            snapshot_id = None

        if 'ALIYUN_DATA_DISK_SIZE' in os.environ:
            data_disk_size = int(os.environ['ALIYUN_DATA_DISK_SIZE'])
        elif snapshot_id:
            data_disk_size = 500
        else:
            data_disk_size = 0

        if snapshot_id or data_disk_size:
            # creating data disk.
            log("creating data disk ..")
            zone_id = instance.zone_id()
            disk = client.create_disk(zone_id=zone_id, snapshot_id=snapshot_id,
                                      disk_name=f"{instance.instance_name()}-data",
                                      size=data_disk_size)
            client.wait_disk_available(disk)
            log(f"data disk of {disk()} is {disk.status()}.")
            while disk.is_avaliable():
                try:
                    client.attach_disk(disk, instance, True)
                except Exception as e:
                    pass
                time.sleep(1)
                client.describe_disk_attribute(disk)
                client.wait_disk_attaching(disk)
            client.wait_disk_in_use(disk)
            log(f"data disk of {disk()} is {disk.status()}.")
            log(f"data disk of {disk()} is in use by instance {instance.instance_name()}.")

        # Sleep 2 sec for instance resource created at once.
        time.sleep(2)

    task = Task(name, instance=instance,
                install_script=install_script,
                image_name=image_name,
                instance_type=instance_type)

    ncluster_globals.register_task(task, run_name)
    return task


def do_interaction(hint: str):
    while (1):
        answer = input(hint)
        if answer.lower() == "yes" or answer.lower() == "y":
            break
        elif answer.lower() == "no" or answer.lower() == "n":
            print("confirm to exit.")
            exit(0)
        else:
            print("input error, please input \"yes\" or \"no\" again.")
            continue


def make_job(
        name: str = '',
        run_name: str = '',
        num_tasks: int = 1,
        install_script: str = '',
        instance_type: str = '',
        image_name: str = '',
        create_resources=True,
        skip_setup=False,
        ssh_auth_for_task0=True,
        spot=False,
        confirm_cost=False,
        **kwargs) -> Job:
    """
  Args:
    skip_setup: True to skip setup
    create_resources: if True, will create resources if necessary
    name: see backend.make_task
    run_name: see backend.make_task
    num_tasks: number of tasks to launch
    install_script: see make_task
    instance_type: see make_task
    image_name: see make_task
  Returns:
    :param

  """

    print(kwargs)

    ncluster_globals.set_should_skip_setup(skip_setup)

    assert num_tasks > 0, f"Can't create job with {num_tasks} tasks"
    assert name.count(
        '.') <= 1, "Job name has too many .'s (see ncluster design: Run/Job/Task hierarchy for  convention)"

    assert instance_type, "Please specify the instance_type"

    current_zone = os.environ.get('ALIYUN_DEFAULT_ZONE', '')
    if not current_zone:
        do_interaction(
            "you don't specify the ALIYUN_DEFAULT_ZONE env, type yes to select the avaliable zone by instance type automatically or no to exit (yes/no)")
    _set_aliyun_environment(instance_type)

    skip_cost_conform = os.environ.get('FASTGPU_SKIP_COST_CONFORM', '')
    if not skip_cost_conform and not confirm_cost:
        do_interaction("Costs will arising from creation of instance/vpc/nas, confirm to continue? (yes/no)")

    # dummy tasks for logging
    tasks = [backend.Task(f"task{i}.{name}") for i in range(num_tasks)]

    if create_resources:
        _maybe_create_resources(tasks[0])

    name = ncluster_globals.auto_assign_job_name_if_needed(name)
    run_name = ncluster_globals.auto_assign_run_name_if_needed(run_name)
    _run = ncluster_globals.create_run_if_needed(run_name, make_run)

    job = Job(name=name, tasks=tasks, run_name=run_name, **kwargs)

    exceptions = []

    # make tasks in parallel
    def make_task_fn(i: int):
        try:
            tasks[i] = make_task(f"{name}{i}", run_name=run_name,
                                 install_script=install_script,
                                 instance_type=instance_type, image_name=image_name,
                                 logging_task=tasks[i],
                                 create_resources=False,
                                 spot=spot,
                                 # handle resources in job already
                                 **kwargs)
        except Exception as e:
            exceptions.append(e)

    # TODO: The parallel call may raise race condition.

    util.log("Creating threads")
    threads = [threading.Thread(name=f'make_task_{i}',
                                target=make_task_fn, args=[i])
               for i in range(num_tasks)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Exception are ", exceptions) if exceptions else None
    if exceptions:
        raise exceptions[0]

    job.tasks = tasks

    if ncluster_globals.should_skip_setup():
        ssh_auth_for_task0 = False
    if ssh_auth_for_task0 and len(job.tasks) > 0:
        ssh_cmd = ['FILE=/root/.ssh/id_rsa',
                   'if [ -f "$FILE" ]; then',
                   '  echo "$FILE exist"',
                   'else',
                   '  cat /dev/zero | ssh-keygen -q -N ""',
                   'fi']
        job.tasks[0].run('\n'.join(ssh_cmd))
        id_rsa_pub, _ = job.tasks[0].run_with_output('cat ~/.ssh/id_rsa.pub')
        for task in job.tasks:
            stdout_str, _ = task.run_with_output('cat ~/.ssh/authorized_keys')
            if id_rsa_pub not in stdout_str:
                task.run(f'echo \"{id_rsa_pub}\" >> ~/.ssh/authorized_keys')
        # license check
        # job.run('python3 /usr/local/licenseCheck.py')

    return job


def make_run(name) -> Run:
    run = Run(name)
    ncluster_globals.register_run(run, name)
    return run


def _maybe_start_instance(instance: aliyun.Instance):
    """Starts instance if it's stopped, no-op otherwise."""

    if not instance:
        return
    client = u.get_ecs_client()
    if instance.is_stopping():
        client.wait_instance_stopped(instance)
    if instance.is_stopped():
        client.start_instance(instance)
        client.wait_instance_running(instance)


'''
def _maybe_wait_for_initializing_instance(instance):
  """Starts instance if it's stopped, no-op otherwise."""

  if not instance:
    return

  if instance.state['Name'] == 'initializing':
    while True:
      print(f"Waiting  for {instance} to leave state 'initializing'.")
      instance.reload()
      if instance.state['Name'] == 'running':
        break
      time.sleep(10)
'''


def _maybe_create_resources(logging_task: Task = None):
    """Use heuristics to decide to possibly create resources"""

    def log(*args):
        if logging_task:
            logging_task.log(*args)
        else:
            util.log(*args)

    def should_create_resources(**kwargs):
        """Check if gateway, keypair, vpc exist."""
        if "vpc_name" in kwargs.keys():
            vpc_name = kwargs.get("vpc_name")
            vpcs = u.get_vpc_dict()
            print(vpcs)
            assert 0
            if vpc_name in vpcs:
                log(f"Using {vpc_name} vpc")
                return False
        else:
            prefix = u.get_prefix()
            vpc_prefix = u.get_vpc_prefix()
            if u.get_keypair_name() not in u.get_keypair_dict():
                log(f"Missing {u.get_keypair_name()} keypair, creating resources")
                return True
            vpcs = u.get_vpc_dict()
            if vpc_prefix not in vpcs:
                log(f"Missing {prefix} vpc, creating resources")
                return True

        if not ncluster_globals.should_disable_nas():
            nass = u.get_nas_dict()
            nas_name = u.get_nas_name()
            if nas_name not in nass:
                log(f"Missing {nas_name} nas, creating resources")
                return True
            if not u.nas_mount_target_match():
                log(f'Mount target mismatch, creating resources')
                return True

        return False

    try:
        if not should_create_resources():
            util.log("Resources already created, no-op")
            # check if current public ip in the authorized group.

            return
        should_disable_nas = ncluster_globals.should_disable_nas()
        create_lib.create_resources(disable_nas=should_disable_nas)
    finally:
        util.log("Resources created.")


def _set_aliyun_environment(instance_type):
    """Sets up ALIYUN environment from NCLUSTER environment variables"""
    current_zone = os.environ.get('ALIYUN_DEFAULT_ZONE', '')
    current_region = os.environ.get('ALIYUN_DEFAULT_REGION', '')

    if current_zone:
        print(f'Using zone of {current_zone}')
    else:
        zones = u.get_zones()
        zone_id = None
        for zone in zones:
            if instance_type in zone.available_instance_types():
                zone_id = zone.zone_id()
                print(f"zone {zone_id} support instance type {instance_type}")
                break
        current_zone = zone_id
        if zone_id == None:
            print(
                f'instance_type:{instance_type} not support in current zones, zone_id:{zone_id}, if you have create the instance before, please specify the ALIYUN_DEFAULT_ZONE envronment')
            exit(0)
        os.environ['ALIYUN_DEFAULT_ZONE'] = current_zone

    if current_region and current_zone:
        assert current_region in current_zone, f'Current zone "{current_zone}" ($ALIYUN_DEFAULT_ZONE) is not ' \
                                               f'in current region "{current_region} ($ALIYUN_DEFAULT_REGION)'

    # zone is set, set region from zone
    if current_zone and not current_region:
        current_region = current_zone[:-2]
        os.environ['ALIYUN_DEFAULT_REGION'] = current_region

    assert current_region, 'Please set the environment $ALIYUN_DEFAULT_REGION for cloud resource.'
    if not current_zone:
        print('Warning: Zone id is not assigned beforehand.')

    util.log(f"Using  region {current_region}, "
             f"zone {current_zone}")
