import json
import time
from .aliyun_client import AliyunClient
from .aliyun_object import FileSystem, MountTarget

class NasClient(AliyunClient):

  def __init__(self, access_key_id, access_key_secret, region_id):
    super().__init__(access_key_id, access_key_secret, region_id)
  
  def describe_regions(self):
    from aliyunsdknas.request.v20170626.DescribeRegionsRequest import DescribeRegionsRequest
    request = DescribeRegionsRequest()
    print(self.action(request))
  
  def describe_zones(self):
    from aliyunsdknas.request.v20170626.DescribeZonesRequest import DescribeZonesRequest
    request = DescribeZonesRequest()
    print(self.action(request))
  
  def describe_file_system_attribute(self, file_system:FileSystem):
    file_system_list = self.describe_file_systems(file_system())
    if len(file_system_list):
      file_system.update_attribute(json.dumps(file_system_list[0].attribute))

  def describe_file_systems(self, file_system_id=None):
    from aliyunsdknas.request.v20170626.DescribeFileSystemsRequest import DescribeFileSystemsRequest
    request = DescribeFileSystemsRequest()
    if file_system_id:
      request.set_FileSystemId(file_system_id)
    response = self.action(request)
    file_system_list = list()
    if response:
      response_json = json.loads(response)
      for file_system_json in response_json['FileSystems']['FileSystem']:
        file_system = FileSystem(attribute_json=file_system_json)
        file_system_list.append(file_system)
    return file_system_list

  def create_file_system(self, description:str, protocol_type='NFS', storage_type='Performance'):
    from aliyunsdknas.request.v20170626.CreateFileSystemRequest import CreateFileSystemRequest
    request = CreateFileSystemRequest()
    request.set_ProtocolType(protocol_type)
    request.set_StorageType(storage_type)
    request.set_Description(description)
    response = self.action(request)
    assert response, 'Create file system failed.'
    response_json = json.loads(response)
    file_system = FileSystem(id=response_json['FileSystemId'])
    self.describe_file_system_attribute(file_system)
    return file_system
  
  def delete_file_system(self, file_system:FileSystem):
    from aliyunsdknas.request.v20170626.DeleteFileSystemRequest import DeleteFileSystemRequest
    request = DeleteFileSystemRequest()
    request.set_FileSystemId(file_system())
    _ = self.action(request)
  
  def describe_mount_target_attribute(self, file_system_id, mount_target:MountTarget):
    mount_target_list = self.describe_mount_targets(file_system_id, mount_target())
    if len(mount_target_list):
      mount_target.update_attribute(json.dumps(mount_target_list[0].attribute))

  def describe_mount_targets(self, file_system_id, mount_target_domain=None):
    from aliyunsdknas.request.v20170626.DescribeMountTargetsRequest import DescribeMountTargetsRequest
    request = DescribeMountTargetsRequest()
    request.set_FileSystemId(file_system_id)
    if mount_target_domain:
      request.set_MountTargetDomain(mount_target_domain)
    response = self.action(request)
    mount_target_list = list()
    if response:
      response_json =json.loads(response)
      for mount_target_json in response_json['MountTargets']['MountTarget']:
        mount_target = MountTarget(attribute_json=mount_target_json)
        mount_target_list.append(mount_target)
    return mount_target_list
  
  def create_mount_target(self, file_system_id, 
                          network_type='Vpc',
                          vpc_id=None,
                          vswitch_id=None,
                          access_group_name='DEFAULT_VPC_GROUP_NAME'):
    from aliyunsdknas.request.v20170626.CreateMountTargetRequest import CreateMountTargetRequest
    request = CreateMountTargetRequest()
    request.set_FileSystemId(file_system_id)
    request.set_NetworkType(network_type)
    if vpc_id:
      request.set_VpcId(vpc_id)
    if vswitch_id:
      request.set_VSwitchId(vswitch_id)
    request.set_AccessGroupName(access_group_name)
    response = self.action(request)
    assert response, 'create mount target failed.'
    response_json = json.loads(response)
    mount_target = MountTarget(response_json['MountTargetDomain'])
    self.describe_mount_target_attribute(file_system_id, mount_target)
    return mount_target
  
  def delete_mount_target(self, file_system_id, mount_target:MountTarget):
    from aliyunsdknas.request.v20170626.DeleteMountTargetRequest import DeleteMountTargetRequest
    request = DeleteMountTargetRequest()
    request.set_FileSystemId(file_system_id)
    request.set_MountTargetDomain(mount_target())
    _ = self.action(request)
  
  def wait_mount_target_active(self, file_system_id, mount_target:MountTarget):
    while not mount_target.is_active():
      time.sleep(1)
      self.describe_mount_target_attribute(file_system_id, mount_target)



  


  
