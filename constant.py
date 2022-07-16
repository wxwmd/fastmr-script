
# TODO: __init__ has imported

# ECS 
INTERNET_CHARGE_TYPE = 'PayByTraffic'

# network
CIDR_BLOCK_MASK = 20  # only 16 zone supported, 4094 IP available for a vsw
IP_DELTA = 1<<(24-CIDR_BLOCK_MASK)

RETRY_TIMEOUT_SEC = 30  # how long to wait before retrying fails

IMAGES_OS_CHOICES = ['aliyun_2','centos', 'ubuntu']

# OSS default 
DEFAULT_MOUNT_DIR = '/ncluster'

# conda yaml file location
CONDA_YAML_DIR = 'https://ali-perseus-release.oss-cn-huhehaote.aliyuncs.com/conda'

LOG_ORDER = ['', 'FATAL','ERROR','WARN','INFO','DEBUG', 'TRACE']

#CUDA
DEFAULT_GPU_DRIVER_VERSION = '460.91.03'
DEFAULT_CUDA_VERSION = '11.2.2'
DEFAULT_CUDNN_VERSION = '8.1.1'


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

AIACC_VM_IMAGE_LIST = ['aiacc-dlimg-ubuntu2004:1.5.0']