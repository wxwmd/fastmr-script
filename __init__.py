import os

from . import aliyun_backend
from . import aliyun_util
from . import util
from . import local_backend
from . import backend  # TODO: remove?

from .ncluster import get_backend
from .ncluster import set_backend
from .ncluster import running_locally

from .ncluster import use_aliyun
from .ncluster import use_local

from .ncluster import make_task
from .ncluster import make_job
from .ncluster import make_run
from .ncluster import get_zone
from .ncluster import get_region
from .ncluster import set_logdir_root
from .ncluster import get_logdir_root
from .ncluster import get_mpi_prefix
from .ncluster import gen_command_file



import pkg_resources  # part of setuptools
#__version__ = pkg_resources.require("ncluster")[0].version
# set default backend from environment
set_backend('aliyun')

# print custom settings
for v in os.environ:
  if v.startswith('NCLUSTER'):
    print(f"ncluster env setting {v}={os.environ[v]}")

if not util.is_set('NCLUSTER_DISABLE_PDB_HANDLER'):
  util.install_pdb_handler()  # CTRL+\ drops into pdb
