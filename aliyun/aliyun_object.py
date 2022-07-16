import json

class Attribute(object):
  attribute: json
  id: str

  def __init__(self, id=None, attribute_json=None):
    if attribute_json:
      self.attribute = attribute_json
    elif id:
      self.id = id
      self.attribute = None
    else:
      self.attribute = None
      self.id = None
  
  def __call__(self):
      return self.id
  
  def update_attribute(self, attribute_str:str):
    if attribute_str:
      self.attribute = json.loads(attribute_str)
    else:
      self.attribute = None
  
  
  def is_valid(self):
    return self.attribute is not None

  def get_attribute(self, arg, *args):
    if self.is_valid():
      try:
        value = self.attribute[arg]
      except Exception as _:
        # print(f'WARNING: {arg} not in the attribute')
        return None
      if len(args) == 0:
        return value
      elif len(args) == 1:
        return value[args[0]]
      elif len(args) == 2:
        return value[args[0]][args[1]]
      elif len(args) == 3:
        return value[args[0]][args[1]][args[2]]
      else:
        raise Exception('Unsupported args size.')
    else:
      raise Exception("Invalid attribute")

class InstanceType(Attribute):
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['InstanceTypeId']
  
  def cpu_core_count(self)->int:
    self.get_attribute('CpuCoreCount')
  
  def eni_private_ip_address_quantity(self)->int:
    self.get_attribute('EniPrivateIpAddressQuantity')
  
  def eni_quantity(self)->int:
    self.get_attribute('EniQuantity')
  
  def gpu_amount(self)->int:
    self.get_attribute('GPUAmount')
  
  def gpu_spec(self)->str:
    self.get_attribute('GPUSpec')
  
  def instance_bandwidth_rx(self)->int:
    self.get_attribute('InstanceBandwidthRx')
  
  def instance_bandwidth_tx(self)->int:
    self.get_attribute('InstanceBandwidthTx')
  
  def instance_family_level(self)->str:
    self.get_attribute('InstanceFamilyLevel')
  
  def instance_pps_rx(self)->int:
    self.get_attribute('InstancePpsRx')
  
  def instance_pps_tx(self)->int:
    self.get_attribute('InstancePpsTx')
  
  def instance_type_family(self)->str:
    self.get_attribute('InstanceTypeFamily')
  
  def instance_type_id(self)->str:
    self.get_attribute('InstanceTypeId')
  
  def local_storage_amount(self)->int:
    self.get_attribute('LocalStorageAmount')
  
  def local_storage_capacity(self)->int:
    self.get_attribute('LocalStorageCapacity')
  
  def local_storage_category(self)->str:
    self.get_attribute('LocalStorageCategory')
  
  def memory_size(self)->int:
    self.get_attribute('MemorySize')
  
class Instance(Attribute):
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()

  def update_id(self):
    if self.attribute:
      self.id = self.attribute['InstanceId']
  
  def creation_time(self):
    return self.get_attribute('CreationTime')

  def start_time(self):
    return self.get_attribute('StartTime')

  def public_ips(self):
    return self.get_attribute('PublicIpAddress', 'IpAddress')
  
  def public_ip(self, index=0):
    value = self.public_ips()
    if len(value):
      return value[index]
    else:
      return str()
  
  def private_ips(self):
    return self.get_attribute('VpcAttributes', 'PrivateIpAddress', 'IpAddress')
  
  def private_ip(self, index=0):
    value = self.private_ips()
    if len(value):
      return value[index]
    else:
      return str()
  
  def key_pair_name(self)->str:
    value = self.get_attribute('KeyPairName')
    if value:
      return value
    else:
      return str()

  def tags(self):
    return self.get_attribute('Tags')

  def vlan_id(self):
    return self.get_attribute('VlanId')
  
  def region_id(self):
    return self.get_attribute('RegionId')
  
  def zone_id(self):
    return self.get_attribute('ZoneId')
  
  def status(self):
    #
    return self.get_attribute('Status')
  
  def is_running(self):
    try:
      value = (self.status() == 'Running')
    except:
      return False
    else:
      return value
  
  def is_stopped(self):
    try:
      value = (self.status() == 'Stopped')
    except:
      return False
    else:
      return value
  
  def is_stopping(self):
    try:
      value = (self.status() == 'Stopping')
    except:
      return False
    else:
      return value
  
  def host_name(self):
    return self.get_attribute('HostName')

  def instance_name(self):
    return self.get_attribute('InstanceName')
  
  def image_id(self):
    return self.get_attribute('ImageId')
  
  def instance_id(self):
    return self.get_attribute('InstanceId')
  
  def instance_type(self):
    return self.get_attribute('InstanceType')
  
  def instance_charge_type(self):
    return self.get_attribute('InstanceChargeType')
  
  def instance_network_type(self):
    return self.get_attribute('InstanceNetworkType')
  
  def internet_charge_type(self):
    return self.get_attribute('InternetChargeType')
  
  def internet_max_bandwidth_in(self):
    return self.get_attribute('InternetMaxBandwidthIn')
  
  def internet_max_bandwidth_out(self):
    return self.get_attribute('InternetMaxBandwidthOut')
  
  def security_group_ids(self):
    return self.get_attribute('SecurityGroupIds', 'SecurityGroupId')
  
  def security_group_id(self, index=0):
    value = self.security_group_ids()
    if len(value):
      return value[index]
    else:
      return str()
  
  def serial_number(self):
    return self.get_attribute('SerialNumber')
  
  def vswitch_id(self):
    return self.get_attribute('VpcAttributes', 'VSwitchId')
  
  def vpc_id(self):
    return self.get_attribute('VpcAttributes', 'VpcId')
  
  def request_id(self):
    return self.get_attribute('RequestId')
  
  def cpu(self):
    return self.get_attribute('Cpu')
  
  def memory(self):
    return self.get_attribute('Memory')
  
  def gpu_amount(self)->int:
    return self.get_attribute('GPUAmount')
  
  def gpu_spec(self)->str:
    return self.get_attribute('GPUSpec')
  
  def deployment_set_id(self)->str:
    return self.get_attribute('DeploymentSetId')


class Zone(Attribute):
  # https://help.aliyun.com/document_detail/25610.html?spm=a2c4g.11186623.6.1201.1c0e35bdlMvcZC
  # https://help.aliyun.com/document_detail/25610.html?spm=a2c4g.11186623.6.1202.3a6f1f3cbjkd1X
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['ZoneId']
  
  def zone_id(self):
    return self.get_attribute('ZoneId')

  def available_disk_categories(self)->list:
    value = self.get_attribute('AvailableDiskCategories')
    if value:
      return value['DiskCategories']
    else:
      return list()

  def available_instance_types(self)->list:
    value = self.get_attribute('AvailableInstanceTypes')
    if value:
      return value['InstanceTypes']
    else:
      return list()


class Image(Attribute):
  # https://help.aliyun.com/document_detail/25534.html?spm=a2c4g.11186623.6.1098.3517431dz27mvk
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['ImageId']
  
  def image_id(self):
    return self.get_attribute('ImageId')
  
  def image_name(self):
    return self.get_attribute('ImageName')
  
  def image_version(self)->str:
    return self.get_attribute('ImageVersion')
  
  def os_name(self)->str:
    return self.get_attribute('OSName')
  
  def os_type(self)->str:
    return self.get_attribute('OSType')
  
  def platform(self)->str:
    return self.get_attribute('Platform')
  


class Disk(Attribute):
  # https://help.aliyun.com/document_detail/104241.html?spm=a2c4g.11186623.6.598.f3fc55d5CKh1wU
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['DiskId']
  
  def disk_id(self):
    return self.get_attribute('DiskId')
  
  def disk_name(self):
    return self.get_attribute('DiskName')
  
  def category(self):
    return self.get_attribute('Category')
  
  def size(self):
    return self.get_attribute('Size')
  
  def status(self):
    # In_use / Available / Attaching /Detaching / Creating / ReIniting
    return self.get_attribute('Status')
  
  def is_avaliable(self)->bool:
    return self.status() == 'Available'
  
  def is_in_use(self)->bool:
    return self.status() == 'In_use'

  
  def image_id(self):
    return self.get_attribute('ImageId')
  
  def instance_id(self):
    return self.get_attribute('InstanceId')
  
  def disk_charge_type(self):
    return self.get_attribute('DiskChargeType')
  
  def delete_with_instance(self)->bool:
    return self.get_attribute('DeleteWithInstance')
  
  def enable_autosnapshot(self)->bool:
    return self.get_attribute('EnableAutoSnapshot')
  
  def device(self):
    return self.get_attribute('Device')
  
  def zone_id(self):
    return self.get_attribute('ZoneId')
  
  def type(self):
    return self.get_attribute('Type')
  
  def portable(self)->bool:
    return self.get_attribute('Portable')

class Snapshot(Attribute):
  # https://help.aliyun.com/document_detail/104268.html?spm=a2c4g.11186623.6.628.5e76da98iAbUoc#h2-url-3
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['SnapshotId']
  
  def snapshot_id(self):
    return self.get_attribute('SnapshotId')
  
  def snapshot_name(self):
    return self.get_attribute('SnapshotName')
  
  def process(self)->str:
    # return in str(%)
    return self.get_attribute('Progress')
  
  def remain_time(self)->int:
    # return in int (sec.)
    return self.get_attribute('RemainTime')
  
  def status(self):
    # return in str: 'progressing' / 'accomplished' / 'failed'
    return self.get_attribute('Status')
  
  def is_accomplished(self):
    return self.status() == 'accomplished'
  
  def is_progressing(self):
    return self.status() == 'progressing'
  
  def is_faied(self):
    return self.status() == 'failed'



class SecurityGroup(Attribute):
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['SecurityGroupId']
  
  def security_group_id(self):
    return self.get_attribute('SecurityGroupId')
  
  def security_group_name(self):
    return self.get_attribute('SecurityGroupName')
  
  def security_group_type(self):
    return self.get_attribute('SecurityGroupType')
  
  def vpc_id(self):
    return self.get_attribute('VpcId')
  
  def tags(self):
    return self.get_attribute('Tags', 'Tag')


class Vpc(Attribute):
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['VpcId']

  def vpc_id(self):
    return self.get_attribute('VpcId')

  def status(self):
    return self.get_attribute('Status')
  
  def region_id(self):
    return self.get_attribute('RegionId')

  def vrouter_id(self):
    return self.get_attribute('VRouterId')
  
  def vswitch_ids(self):
    return self.get_attribute('VSwitchIds', 'VSwitchId')

  def vswitch_id(self, zone_id: str = ''):
    value = self.vswitch_ids()
    if len(value):
      if zone_id:
        vswitchs = [VSwitch(id) for id in value]
        for vswitch in vswitchs:
          if vswitch.zone_id() == zone_id:
            return vswitch.vswitch_id()
      else:
        return value[0]
    else:
      return str()


  def vpc_name(self):
    return self.get_attribute('VpcName')

  def cidr_block(self):
    return self.get_attribute('CidrBlock')


class VSwitch(Attribute):
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['VSwitchId']
  
  def vswitch_id(self):
    return self.get_attribute('VSwitchId')
  
  def status(self):
    return self.get_attribute('Status')
  
  def cidr_block(self):
    return self.get_attribute('CidrBlock')
  
  def resource_group_id(self):
    return self.get_attribute('ResourceGroupId')
  
  def route_table_id(self):
    return self.get_attribute('RouteTable', 'RouteTableId')
  
  def vswitch_name(self):
    return self.get_attribute('VSwitchName')
  
  def vpc_id(self):
    return self.get_attribute('VpcId')
  
  def zone_id(self):
    return self.get_attribute('ZoneId')


class KeyPair(Attribute):
  def __init__(self, attribute_json=None):
    super().__init__(id, attribute_json)
  
  def key_pair_finger_print(self):
    return self.get_attribute('KeyPairFingerPrint')
  
  def key_pair_name(self):
    return self.get_attribute('KeyPairName')
  
  def resource_group_id(self):
    return self.get_attribute('ResouceGroupId')
  
  def tags(self):
    return self.get_attribute('Tags')


class FileSystem(Attribute):
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['FileSystemId']
  
  def description(self):
    return self.get_attribute('Description')
  
  def file_system_id(self):
    return self.get_attribute('FileSystemId')

  def metered_size(self):
    return self.get_attribute('MeteredSize')
  
  def mount_targets(self):
    return self.get_attribute('MountTargets', 'MountTarget')
  
  def mount_target(self, index=0):
    targets = self.mount_targets()
    if len(targets) > index:
      return targets[index]
    else:
      return None
  
  def mount_target_domain(self, index=0):
    target = self.mount_target(index)
    if target:
      return target['MountTargetDomain']
    else:
      return None
  
  def packages(self):
    return self.get_attribute('Packages')
  
  def package_id(self):
    'TODO'

  def protocol_type(self):
    return self.get_attribute('ProtocolType')
  
  def region_id(self):
    return self.get_attribute('RegionId')
  
  def storage_type(self):
    return self.get_attribute('StorageType')

class MountTarget(Attribute):
  def __init__(self, id=None, attribute_json=None):
    super().__init__(id, attribute_json)
    self.update_id()
  
  def update_id(self):
    if self.attribute:
      self.id = self.attribute['MountTargetDomain']
  
  def access_group(self):
    return self.get_attribute('AccessGroup')
  
  def network_type(self):
    return self.get_attribute('NetworkType')
  
  def status(self):
    # Active/Inactive/Creating
    return self.get_attribute('Status')
  
  def is_active(self):
    return self.status() == 'Active'
  
  def vpc_id(self):
    return self.get_attribute('VpcId')
  
  def vswitch_id(self):
    return self.get_attribute('VswId')
  
  
