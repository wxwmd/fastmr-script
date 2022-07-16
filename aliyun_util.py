"""Methods used in aliyun_backend, but also useful for standalone prototyping in Jupyter"""

import os
import re
import sys
import time
from collections import OrderedDict

import oss2
import paramiko
from operator import itemgetter

from typing import Iterable, List

import util
import aliyun

EMPTY_NAME = "noname"  # name to use when name attribute is missing on Aliyun
RETRY_INTERVAL_SEC = 1  # how long to wait before retries
RETRY_TIMEOUT_SEC = 30  # how long to wait before retrying fails
DEFAULT_PREFIX = 'ncluster'
PRIVATE_KEY_LOCATION = os.environ['HOME'] + '/.ncluster'
DUPLICATE_CHECKING = False
DEFAULT_VPC_PREFIX = 'fastmr-vpc'


def get_vpc():
    """
  Returns current VPC object
  """

    return get_vpc_dict()[get_vpc_prefix()]


def get_vpc_from_instance(instance: aliyun.Instance):
    """
  Return VPC object from instance.
  """
    vpc_id = instance.vpc_id()
    vpc_client = get_vpc_client()
    vpc = aliyun.Vpc(id=vpc_id)
    vpc_client.describe_vpc_attribute(vpc)
    return vpc


def get_vswitch():
    """
    Return current vswitch object
    """
    vpc = get_vpc()
    return get_vswitch_dict(vpc)[get_vswitch_name()]


# def get_vswitch(zone=None):
#     """
#     Return current vswitch object
#     """
#     from . import aliyun_create_resources
#     vpc = get_vpc()
#     if get_vswitch_name(zone) not in get_vswitch_dict(vpc):
#         aliyun_create_resources.create_vswitch()
#     return get_vswitch_dict(vpc)[get_vswitch_name()]


def get_nas():
    """
    Return current nas object
    """
    return get_nas_dict()[get_nas_name()]


def get_mount_target():
    """
    Return current mount target object
    """
    vswitch = get_vswitch()
    return get_mount_target_dict()[vswitch()]


def get_security_group():
    """
  Returns current security group
  """
    vpc = get_vpc()
    return get_security_group_dict()[vpc.vpc_name()]


def get_security_group_by_vpcid():
    """
  Returns current security group by vpc_id
  """
    vpc = get_vpc()
    return get_security_group_dict()[vpc.vpc_id()]


def get_vpc_dict():
    """Returns dictionary of named VPCs {name: vpc}

  Assert fails if there's more than one VPC with same name."""

    client = get_vpc_client()
    vpc_list = client.describe_vpcs()

    result = OrderedDict()
    for vpc in vpc_list:
        key = vpc.vpc_name()
        if not key or key == EMPTY_NAME:  # skip VPC's that don't have a name assigned
            continue

        if key in result:
            util.log(f"Warning: Duplicate VPC group {key} in {result}")
            if DUPLICATE_CHECKING:
                assert False
        result[key] = vpc

    return result


# def get_vswitch_dict(vpc):
#     """Returns dictionary of named gateways for given VPC {name: gateway}"""
#     client = get_vpc_client()
#     vswitch_list = client.describe_vswitches(vpc())
#     result = OrderedDict()
#     print("result = ", result)
#     for vswitch in vswitch_list:
#         # key = vswitch.vswitch_name()
#         key = vswitch.zone_id()
#         print("key = ", key)
#         if not key or key == EMPTY_NAME:
#             continue
#         assert key not in result
#         result[key] = vswitch
#
#     return result

def get_vswitch_dict(vpc):
    """Returns dictionary of named gateways for given VPC {name: gateway}"""
    client = get_vpc_client()
    vswitch_list = client.describe_vswitches(vpc())
    result = OrderedDict()
    for vswitch in vswitch_list:
        # key = vswitch.vswitch_name()
        key = vswitch.zone_id()
        if not key or key == EMPTY_NAME:
            continue
        if key in result:
            continue
        result[key] = vswitch

    return result


def get_nas_dict():
    """Returns dictionary of {desciption_name: file_system}"""

    client = get_nas_client()
    file_system_list = client.describe_file_systems()
    result = OrderedDict()
    for file_system in file_system_list:
        # print(f'!!!! {file_system.attribute}')
        key = file_system.description()
        if not key or key == EMPTY_NAME:  # skip NAS without a name
            continue
        assert key not in result
        result[key] = file_system

    return result


def get_mount_target_dict():
    """Returns dictionary of {vswitch_id: mount_target_domain}"""

    client = get_nas_client()
    nas = get_nas()
    result = OrderedDict()
    for mount_target in nas.mount_targets():
        mount_target_domain_id = mount_target['MountTargetDomain']
        mount_target_domain = aliyun.MountTarget(id=mount_target_domain_id)
        client.describe_mount_target_attribute(nas(), mount_target_domain)
        key = mount_target_domain.vswitch_id()
        if not key or key == EMPTY_NAME:
            continue
        assert key not in result
        result[key] = mount_target_domain

    return result


def get_security_group_dict():
    """Returns dictionary of named security groups {name: securitygroup}."""

    client = get_ecs_client()
    vpc_client = get_vpc_client()
    security_group_list = client.describe_security_groups()

    result = OrderedDict()
    for security_group in security_group_list:
        vpc_id = security_group.vpc_id()
        # vpc = aliyun.Vpc(id=vpc_id)
        # vpc_client.describe_vpc_attribute(vpc)
        key = vpc_id
        # key = vpc.vpc_name()
        if not key or key == EMPTY_NAME:
            continue  # ignore unnamed security groups
        #    key = security_group_response['GroupName']
        if key in result:
            # util.log(f"Warning: Duplicate security group {key}")
            if DUPLICATE_CHECKING:
                assert key not in result, ("Duplicate security group " + key)
        result[key] = security_group

    return result


def get_keypair_dict():
    """Returns dictionary of {keypairname: keypair}"""

    client = get_ecs_client()
    keypair_list = client.describe_key_pairs()

    result = {}
    for keypair in keypair_list:
        keypair_name = keypair.key_pair_name()
        if keypair_name in result:
            util.log(f"Warning: Duplicate key {keypair_name}")
        if DUPLICATE_CHECKING:
            assert keypair_name not in result, "Duplicate key " + keypair_name
        result[keypair_name] = keypair
    return result


def get_keypair(keypair_name):
    client = get_ecs_client()
    keypair_list = client.describe_key_pairs(keypair_name)
    if len(keypair_list) > 1:
        util.log(f"Warning: Duplicate key {keypair_name}")
    if keypair_list:
        return keypair_list[0]
    else:
        return None


def get_prefix():
    """Global prefix to identify ncluster created resources name used to identify ncluster created resources,
  (name of NAS, keypair prefixes), can be changed through $NCLUSTER_PREFIX for debugging purposes. """

    name = os.environ.get('NCLUSTER_PREFIX', DEFAULT_PREFIX)
    if name != DEFAULT_PREFIX:
        validate_prefix(name)
    return name


def get_vpc_prefix():
    """Global prefix to identify ncluster created resources name used to identify ncluster created resources,
  (name of VPC), can be changed through $ALIYUN_VPC_PREFIX for debugging purposes. """

    name = os.environ.get('ALIYUN_VPC_PREFIX', DEFAULT_VPC_PREFIX)
    if name != DEFAULT_VPC_PREFIX:
        validate_run_name(name)
    return name


def get_region() -> str:
    return get_session().default_region


def get_zone() -> str:
    """Returns current zone, or empty string if it's unset."""
    return os.environ.get('ALIYUN_DEFAULT_ZONE', '')


def get_zones():
    client = get_ecs_client()
    zone_list = client.describe_zones()
    return zone_list


def get_session():
    return aliyun.Session()


################################################################################
# keypairs
################################################################################
# For naming conventions, see
# https://docs.google.com/document/d/14-zpee6HMRYtEfQ_H_UN9V92bBQOt0pGuRKcEJsxLEA/edit#heading=h.45ok0839c0a

def get_keypair_name():
    """Returns current keypair name."""

    username = get_username()
    assert '-' not in username, "username must not contain -, change $USER"
    validate_aliyun_name(username)
    assert len(username) < 30  # to avoid exceeding Aliyun 127 char limit
    return get_prefix() + '-' + username


def get_keypair_default():
    """Returns current keypair
  """

    return get_keypair_dict()[get_keypair_name()]


def get_keypair_fn():
    """Location of .pem file for current keypair"""

    keypair_name = get_keypair_name()
    region = get_region()
    fn = f'{PRIVATE_KEY_LOCATION}/{keypair_name}-{region}.pem'
    return fn


def get_vpc_name():
    return get_vpc_prefix()


def get_security_group_name():
    # We have two security groups, ncluster for manually created VPC and
    # ncluster-default for default VPC. Once default VPC works for all cases, can
    # get rid of one of security groups
    return get_vpc_prefix()


def get_security_group_id():
    vpc = get_vpc()
    return vpc.vpc_id()


def get_vswitch_name():
    return get_zone()


def get_route_table_name():
    return get_vpc_prefix()


def get_nas_name():
    username = get_username()
    return get_prefix() + '-' + username


def get_username():
    assert 'USER' in os.environ, "why isn't USER defined?"
    return os.environ['USER']


def lookup_image(wildcard):
    """Returns unique ec2.Image whose name matches wildcard

  Assert fails if multiple images match or no images match.
  """

    client = get_ecs_client()
    # TODO: check if reasonable.
    images = client.describe_images(image_name=wildcard)

    # Note, can add filtering by Owners as follows
    #  images = list(ec2.images.filter_(Filters = [filter_], Owners=['self', 'amazon']))

    assert len(images) <= 1, "Multiple images match " + str(wildcard)
    assert len(images) > 0, "No images match " + str(wildcard)
    return images[0]


def lookup_instance(name: str, instance_type: str = '', image_name: str = '', keypair_name: str = ''):
    """Looks up ALIYUN instance for given instance name, like
   simple.worker. If no instance found in current ALIYUN environment, returns None. """

    client = get_ecs_client()
    image = lookup_image(image_name)
    instances = client.describe_instances(instance_name=name, instance_type=instance_type, image_id=image(),
                                          keypair_name=keypair_name)
    # TODO: NEED Check data disk and snapshot.
    assert len(instances) < 2, f"Found two instances with name {name}"
    if not instances:
        return None
    else:
        return instances[0]


# TODO
def lookup_instances(fragment='', verbose=True, filter_by_key=False, valid_states=('Running',),
                     limit_to_current_user=False, exact_match=False) -> List:
    """Returns List of ecs.Instance object whose name contains fragment, in reverse order of launching (ie,
  most recent instance first). Optionally filters by key, only including instances launched with
  key_name matching current username.

  args:
    verbose: print information about all matching instances found

    filter_by_key  if True, ignore instances that are not launched with current
        user's default key
    limit_to_current_user Restrict result to instances that current user can ssh into

  """
    from dateutil import parser
    '''
  if fragment.startswith("'"):
    assert fragment.endswith("'")
    exact_match = True
    fragment = fragment[1:-1]
  else:
    exact_match = False
  '''
    last_name_match = False
    if fragment.startswith("{"):
        assert fragment.endswith("}"), 'The match fragment start with "{" , but doesn\'t end with "}".'
        last_name_match = True
        exact_match = False
        fragment = fragment[1:-1]

    def vprint(*args):
        if verbose:
            print(*args)

    region = get_region()
    client = get_ecs_client()
    instances = client.describe_instances(
        keypair_name=get_keypair_name()) if limit_to_current_user else client.describe_instances()
    instance_list = []
    for instance in instances:
        if instance.status() not in valid_states:
            continue
        # if limit_to_current_user and instance.key_pair_name() != get_keypair_name():
        #   continue

        if exact_match:
            if fragment == instance.instance_name():
                instance_list.append((util.toseconds(parser.parse(instance.start_time())), instance))
        else:
            if last_name_match:
                if fragment == instance.instance_name()[-len(fragment):]:
                    instance_list.append((util.toseconds(parser.parse(instance.start_time())), instance))
            else:
                if fragment in instance.instance_name():
                    # the following can be re-enabled for broader match
                    # or fragment in str(instance.public_ip_address) or
                    # fragment in str(instance.id) or fragment in str(instance.private_ip_address)):
                    instance_list.append((util.toseconds(parser.parse(instance.start_time())), instance))

    sorted_instance_list = reversed(sorted(instance_list, key=itemgetter(0)))

    filtered_instance_list = []  # filter by key
    # vprint("Using region ", region)

    for (ts, instance) in sorted_instance_list:
        if filter_by_key and instance.key_pair_name() != get_keypair_name():
            vprint(f"Got key {instance.key_name}, expected {get_keypair_name()}")
            continue
        filtered_instance_list.append(instance)

    return filtered_instance_list


def ssh_to_task(task) -> paramiko.SSHClient:
    """Create ssh connection to task's machine

  returns Paramiko SSH client connected to host.

  """

    username = task.ssh_username
    hostname = task.public_ip
    ssh_key_fn = get_keypair_fn()
    print(f"ssh -i {ssh_key_fn} {username}@{hostname}")
    pkey = paramiko.RSAKey.from_private_key_file(ssh_key_fn)

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    assert ssh_client

    counter = 1
    while True:
        try:
            ssh_client.connect(hostname=hostname, username=username, pkey=pkey)
            if counter % 11 == 0:  # occasionally re-obtain public ip, machine could've gotten restarted
                hostname = task.public_ip
            break
        except Exception as e:
            if '[Errno None] Unable to connect to port 22 on' in str(e):
                print(f'Waiting... Need more time to connect to {task.name} the first time.')
            else:
                print(f'{task.name}: Exception connecting to {hostname} via ssh (could be a timeout): {e}')
            time.sleep(RETRY_INTERVAL_SEC)

    return ssh_client


def parse_key_name(keyname):
    """keyname => resource, username"""
    # Relies on resource name not containing -, validated in
    # validate_resource_name
    toks = keyname.split('-')
    if len(toks) != 2:
        return None, None  # some other keyname not launched by nexus
    else:
        return toks


aliyun_name_regexp = re.compile('^[a-zA-Z0-9+-=._:/@]*$')


def validate_aliyun_name(name):
    """Validate resource name using Aliyun name restrictions from # """
    assert len(name) <= 127
    # disallow unicode characters to avoid pain
    assert name == name.encode('ascii').decode('ascii')
    assert aliyun_name_regexp.match(name)


resource_regexp = re.compile('^[a-z0-9]+$')


def validate_prefix(name):
    """Check that name is valid as substitute for default prefix. Since it's used in unix filenames, key names, be more conservative than Aliyun requirements, just allow 30 chars, lowercase only."""
    assert len(name) <= 30
    assert resource_regexp.match(name)
    validate_aliyun_name(name)


def validate_run_name(name):
    """Name used for run. Used as part of instance name, tmux session name."""
    assert len(name) <= 30
    validate_aliyun_name(name)


def create_name_tags(name):
    """Returns [{'Key': 'Name', 'Value': name}] """
    return [{'Key': 'Name', 'Value': name}]


def create_nas(name):
    nas_client = get_nas_client()

    token = str(int(time.time() * 1e6))  # epoch usec
    nas = nas_client.create_file_system(description=name)
    time.sleep(RETRY_TIMEOUT_SEC)

    # make sure NAS is now visible
    nas_dict = get_nas_dict()
    assert name in nas_dict
    return nas_dict[name]


def extract_attr_for_match(items, **kwargs):
    """Helper method to get attribute value for an item matching some criterion.
  Specify target criteria value as dict, with target attribute having value -1

  Example:
    to extract state of vpc matching given vpc id

  response = [{'State': 'available', 'VpcId': 'vpc-2bb1584c'}]
  extract_attr_for_match(response, State=-1, VpcId='vpc-2bb1584c') #=> 'available'"""

    # find the value of attribute to return
    query_arg = None
    for arg, value in kwargs.items():
        if value == -1:
            assert query_arg is None, "Only single query arg (-1 valued) is allowed"
            query_arg = arg
    result = []

    filterset = set(kwargs.keys())
    for item in items:
        match = True
        assert filterset.issubset(
            item.keys()), "Filter set contained %s which was not in record %s" % (
            filterset.difference(item.keys()),
            item)
        for arg in item:
            if arg == query_arg:
                continue
            if arg in kwargs:
                if item[arg] != kwargs[arg]:
                    match = False
                    break
        if match:
            result.append(item[query_arg])
    assert len(result) <= 1, "%d values matched %s, only allow 1" % (
        len(result), kwargs)
    if result:
        return result[0]
    return None


def get_tags(instance: aliyun.Instance):
    """Returns instance tags."""
    return instance.tags()


def get_public_ip(instance: aliyun.Instance) -> str:
    return instance.public_ip()


def get_ip(instance: aliyun.Instance) -> str:
    return instance.private_ip()


def call_with_retries(method, debug_string='',
                      retry_interval_sec=RETRY_INTERVAL_SEC,
                      **kwargs):
    while True:
        try:
            value = method(**kwargs)
            assert value is not None, f"{debug_string} was None"
            break
        except Exception as e:
            print(f"{debug_string} failed with {e.__class__}({e}), retrying")
            time.sleep(retry_interval_sec)
            continue

    return value


def get_ecs_client():
    try:
        client = get_session().get_ecs_client()
    except Exception as e:
        print(f"Failed with error '{e}'")
        print("To specify Virginia region, do 'export ALIYUN_DEFAULT_REGION=us-east-1'")
        sys.exit()
    return client


def get_nas_client():
    while True:
        try:
            return get_session().get_nas_client()
        except Exception as e:
            # can get following
            # botocore.exceptions.DataNotFoundError: Unable to load data for: endpoints
            util.log(f"get_session().client('efs') failed with {e}, retrying")
            time.sleep(2)


def get_vpc_client():
    while True:
        try:
            return get_session().get_vpc_client()
        except Exception as e:
            # can get following
            # botocore.exceptions.DataNotFoundError: Unable to load data for: endpoints
            util.log(f"vpc failed with {e}, retrying")
            time.sleep(2)


def nas_mount_target_match() -> bool:
    try:
        get_mount_target()
        return True
    except Exception as e:
        return False


def getossclient():
    access_key_id = os.getenv('ALIYUN_ACCESS_KEY_ID', '<你的AccessKeyId>')
    access_key_secret = os.getenv('ALIYUN_ACCESS_KEY_SECRET', '<你的AccessKeySecret>')
    bucket_name = os.getenv('OSS_TEST_BUCKET', 'mracctest')
    endpoint = os.getenv('OSS_TEST_ENDPOINT', 'http://oss-cn-beijing.aliyuncs.com')
    # 确认上面的参数都填写正确了
    for param in (access_key_id, access_key_secret, bucket_name, endpoint):
        assert '<' not in param, '请设置参数：' + param

    # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
    bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)
    return bucket


def ossupload(ossfile, localfile):
    access_key_id = os.getenv('ALIYUN_ACCESS_KEY_ID', '<你的AccessKeyId>')
    access_key_secret = os.getenv('ALIYUN_ACCESS_KEY_SECRET', '<你的AccessKeySecret>')
    bucket_name = os.getenv('OSS_TEST_BUCKET', 'mracctest')
    endpoint = os.getenv('OSS_TEST_ENDPOINT', 'http://oss-cn-beijing.aliyuncs.com')
    # 确认上面的参数都填写正确了
    for param in (access_key_id, access_key_secret, bucket_name, endpoint):
        assert '<' not in param, '请设置参数：' + param

    # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
    bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)
    bucket.put_object_from_file(ossfile, localfile)


def ssh_to_task_by_password(task) -> paramiko.SSHClient:
    """Create ssh connection to task's machine by password

  returns Paramiko SSH client connected to host.

  """
    username = task.username
    hostname = task.public_ip
    password = task.password
    print(f"ssh {username}@{hostname}")

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    assert ssh_client

    counter = 1
    while True:
        try:
            ssh_client.connect(hostname=hostname, username=username, password=password)
            if counter % 11 == 0:  # occasionally re-obtain public ip, machine could've gotten restarted
                hostname = task.public_ip
            break
        except Exception as e:
            if '[Errno None] Unable to connect to port 22 on' in str(e):
                print(f'Waiting... Need more time to connect to {task.name} the first time.')
            else:
                print(f'{task.name}: Exception connecting to {hostname} via ssh (could be a timeout): {e}')
            time.sleep(RETRY_INTERVAL_SEC)

    return ssh_client


def check_system_version_from_str(res_cmd):
    lsb_list = ['redhat', 'suse', 'debian', 'ubuntu', 'centos', 'aliyunlinux', 'alibabacloud']
    for lsb in lsb_list:
        if lsb in res_cmd.lower():
            return lsb
    return None


'''


def instance_supports_100gbps_network(instance_type: str) -> bool:
  assert_is_valid_instance(instance_type)
  return instance_type == 'p3dn.24xlarge'


def assert_is_valid_instance(instance_type: str):
  # TODO(y): check that instance type is correct to catch common errors
  pass


def create_spot_instances(launch_specs, spot_price=26, expiration_mins=15):
    """
    args:
      spot_price: default is $26 which is right above p3.16xlarge on demand price
      expiration_mins: this request only valid for this many mins from now
    """
    ec2c = get_ec2_client()

    num_tasks = launch_specs['MinCount'] or 1
    if 'MinCount' in launch_specs: del launch_specs['MinCount']
    if 'MaxCount' in launch_specs: del launch_specs['MaxCount']
    tags = None
    if 'TagSpecifications' in launch_specs: 
      try: tags = launch_specs['TagSpecifications'][0]['Tags']
      except: pass
      del launch_specs['TagSpecifications']

    import pytz      # datetime is not timezone aware, use pytz to fix
    import datetime as dt
    now = dt.datetime.utcnow().replace(tzinfo=pytz.utc)

    spot_args = {}
    spot_args['LaunchSpecification'] = launch_specs
    spot_args['SpotPrice'] = str(spot_price)
    spot_args['InstanceCount'] = num_tasks
    spot_args['ValidUntil'] = now + dt.timedelta(minutes=expiration_mins)

    try:
      spot_requests = ec2c.request_spot_instances(**spot_args)
    except Exception as e:
      assert False, f"Spot instance request failed (out of capacity?), error was {e}"
      
    spot_requests = spot_requests['SpotInstanceRequests']
    instance_ids = wait_on_fulfillment(ec2c, spot_requests)
    
    print('Instances fullfilled...')
    ec2 = get_ec2_resource()
    instances = list(ec2.instances.filter(Filters=[{'Name': 'instance-id', 'Values': list(filter(None, instance_ids))}]))

    if not all(instance_ids):
      for i in instances: 
        i.terminate()
      raise RuntimeError('Failed to create spot instances:', instance_ids)

    if tags:
      for i in instances:
          i.create_tags(Tags=tags)

    return instances
'''
