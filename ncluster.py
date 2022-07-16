import aliyun_backend
import local_backend
import fastmr_backend
import backend
import aliyun_util as u
import collections

import ncluster_globals

_backend: type(backend) = backend


def get_logdir_root() -> str:
  return ncluster_globals.LOGDIR_ROOT


def set_logdir_root(logdir_root):
  """Globally changes logdir root for all runs."""
  ncluster_globals.LOGDIR_ROOT = logdir_root


def set_backend(backend_name: str):
  """Sets backend (local or aliyun)"""
  global _backend, _backend_name
  _backend_name = backend_name

  assert not ncluster_globals.task_launched, "Not allowed to change backend after launching a task (this pattern is error-prone)"
  if backend_name == 'aliyun':
    _backend = aliyun_backend
  elif backend_name == 'local':
    _backend = local_backend
  elif backend_name == 'fastmr':
    _backend = fastmr_backend
  else:
    assert False, f"Unknown backend {backend_name}"

  # take default value for logdir root from backend
  ncluster_globals.LOGDIR_ROOT = _backend.DEFAULT_LOGDIR_ROOT


def use_aliyun():
  set_backend('aliyun')


def use_local():
  set_backend('local')


def get_backend() -> str:
  """Returns backend name, ie "local" or "aliyun" """
  return _backend_name


def get_backend_module() -> backend:
  return _backend


def running_locally():
  return get_backend() == 'local'


def get_region() -> str:
  if _backend != local_backend:
    return u.get_region()
  else:
    return 'local'


def get_zone() -> str:
  if _backend != local_backend:
    return u.get_zone()
  else:
    return 'local'


#  def make_run(name='', **kwargs):
#  return _backend.Run(name, **kwargs)


# Use factory methods task=create_task instead of relying solely on constructors task=Task() because underlying hardware resources may be reused between instantiations
# For instance, one may create a Task initialized with an instance that was previous created for this kind of task
# Factory method will make the decision to recreate or reuse such resource, and wrap this resource with a Task object.
def make_task(name: str = '',
              run_name: str = '',
              install_script: str = '',
              **kwargs) -> backend.Task:
  return _backend.make_task(name=name, run_name=run_name,
                            install_script=install_script, **kwargs)


def make_job(name: str = '',
             run_name: str = '',
             num_tasks: int = 0,
             install_script: str = '',
             **kwargs
             ) -> backend.Job:
  """
  Create a job using current backend. Blocks until all tasks are up and initialized.

  Args:
    name: name of the job
    run_name: name of the run (auto-assigned if empty)
    num_tasks: number of tasks
    install_script: bash-runnable script
    **kwargs:

  Returns:
    backend.Job
  """
  return _backend.make_job(name=name, run_name=run_name, num_tasks=num_tasks,
                           install_script=install_script, **kwargs)


def make_run(name: str = '', **kwargs) -> backend.Run:
  return _backend.make_run(name=name, **kwargs)


# TODO: remove?
def join(things_to_join):
  if isinstance(things_to_join, collections.Iterable):
    for thing in things_to_join:
      thing.join()
  else:
    things_to_join.join()


def get_mpi_prefix(np_param, npernode_param, host_param):
  mpi_prefix = f'mpirun --allow-run-as-root -np {np_param} --npernode {npernode_param} --host {host_param} ' \
            '--bind-to none -x NCCL_SOCKET_IFNAME=^lo,docker0 -mca btl_tcp_if_exclude lo,docker0 ' \
            '-x PATH -x XLA_FLAGS -x PYTHONPATH -x LD_LIBRARY_PATH '
  return mpi_prefix


def gen_command_file(command_str, command_file_name='command.sh', display_rank0=True):
  with open(command_file_name, 'w') as f:
    f.write('COMMAND=\''+command_str+'\'\n')
    if display_rank0:
      f.write('if [ $OMPI_COMM_WORLD_RANK -eq 0 ] ; then\n')
      f.write('$COMMAND\n')
      f.write('else\n')
      f.write('$COMMAND >> /dev/null 2>&1\n')
      f.write('fi\n')
    else:
      f.write('$COMMAND\n')
  return command_file_name
