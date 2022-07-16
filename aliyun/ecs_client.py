import json
import time
from .aliyun_object import Image, Instance, InstanceType, SecurityGroup, Vpc, Disk, Snapshot, KeyPair, Zone
from .aliyun_client import AliyunClient


class EcsClient(AliyunClient):

    def __init__(self, access_key_id, access_key_secret, region_id):
        super().__init__(access_key_id, access_key_secret, region_id)
        self.region_id = region_id

    def describe_zones(self):
        # https://help.aliyun.com/document_detail/25610.html?spm=a2c4g.11186623.6.1201.1c0e35bdlMvcZC
        from aliyunsdkecs.request.v20140526.DescribeZonesRequest import DescribeZonesRequest
        request = DescribeZonesRequest()
        response = self.action(request)
        zone_list = list()
        if response:
            response_json = json.loads(response)
            for zone_json in response_json['Zones']['Zone']:
                zone = Zone(attribute_json=zone_json)
                zone_list.append(zone)
        return zone_list

    def describe_images(self, image_id=None,
                        image_name=None,
                        os_type='linux',
                        image_owner_alias=None,
                        architecture=None,
                        page_size=50):
        # https://help.aliyun.com/document_detail/25534.html?spm=a2c4g.11186623.6.1098.3517431dz27mvk
        from aliyunsdkecs.request.v20140526.DescribeImagesRequest import DescribeImagesRequest
        request = DescribeImagesRequest()
        if not image_name:
            region_to_imageId = {'cn-shenzhen': 'm-wz99zr2mst7karw7dnqe',
                                 'cn-shanghai': 'm-uf6ioqf1qlin20qyx0kc',
                                 'cn-huhehaote': 'm-hp3atus0k3unq8sihie6',
                                 'cn-hangzhou': 'm-bp17b0axkx9h9mtcprec',
                                 'cn-hongkong': 'm-j6c7xad8fleyfk2bjuzg',
                                 'cn-qingdao': 'm-m5e0dlm8r7bfj2npab87',
                                 'cn-beijing': 'm-2ze87tgql748p9i996kj',
                                 'cn-zhangjiakou': 'm-8vb1fy0fx1frqiazezid'}
            assert self.region_id in region_to_imageId, "image not support {} region".format(self.region_id)
            default_image_id = region_to_imageId[self.region_id]
            request.set_ImageId(default_image_id)
            request.set_ImageOwnerAlias('marketplace')
            print('use default image id:', default_image_id)
        else:
            if image_id:
                request.set_ImageId(image_id)
            if image_name:
                request.set_ImageName(image_name)
            if architecture:
                request.set_Architecture(architecture)
            if image_owner_alias:
                request.set_ImageOwnerAlias(image_owner_alias)

        request.set_OSType(os_type)
        request.set_PageSize(page_size)
        response = self.action(request)
        image_list = list()
        if response:
            response_json = json.loads(response)
            for image_json in response_json['Images']['Image']:
                image = Image(attribute_json=image_json)
                image_list.append(image)
        return image_list

    def describe_instance_types(self):
        from aliyunsdkecs.request.v20140526.DescribeInstanceTypesRequest import DescribeInstanceTypesRequest
        request = DescribeInstanceTypesRequest()
        response = self.action(request)
        instance_type_list = list()
        if response:
            response_json = json.loads(response)
            for instance_type_json in response_json['InstanceTypes']['InstanceType']:
                instance_type = InstanceType(attribute_json=instance_type_json)
                instance_type_list.append(instance_type)
        return instance_type_list

    # aliyun instance openapi doc: https://help.aliyun.com/document_detail/104199.html?spm=a2c4g.11186623.6.559.59badab1tQPwjz
    def describe_instance_attribute(self, instance):
        from aliyunsdkecs.request.v20140526.DescribeInstanceAttributeRequest import DescribeInstanceAttributeRequest
        request = DescribeInstanceAttributeRequest()
        request.set_accept_format('json')
        request.set_InstanceId(instance())
        response = self.action(request)
        instance.update_attribute(response)

    def describe_instances(self, instance_name=None,
                           instance_ids=None,
                           instance_type=None,
                           image_id=None,
                           keypair_name=None,
                           public_ip_addresses=None,
                           page_size=50):
        from aliyunsdkecs.request.v20140526.DescribeInstancesRequest import DescribeInstancesRequest
        request = DescribeInstancesRequest()
        request.set_accept_format('json')
        if instance_name:
            request.set_InstanceName(instance_name)
        if instance_ids:
            request.set_InstanceIds(instance_ids)
        if instance_type:
            request.set_InstanceType(instance_type)
        if image_id:
            request.set_ImageId(image_id)
        if public_ip_addresses:
            request.set_PublicIpAddresses(public_ip_addresses)
        if keypair_name:
            request.set_KeyPairName(keypair_name)
        request.set_PageSize(page_size)

        response = self.action(request)
        instance_list = list()
        if response:
            response_json = json.loads(response)
            for instance_json in response_json['Instances']['Instance']:
                instance = Instance(attribute_json=instance_json)
                instance_list.append(instance)

        return instance_list

    def create_instances(self, instance_name,
                         vpc_id,
                         vswitch_id,
                         amount=1,
                         host_name=None,
                         instance_type='ecs.gn5-c8g1.2xlarge',
                         image_id='mracc-opensourcev0.1',
                         security_group_id=None,
                         password=None,
                         keypair_name=None,
                         unique_suffix=True,
                         internet_charge_type='PayByBandwidth',
                         internet_max_bandwidth_out=50,
                         io_optimized='optimized',
                         system_disk_size='500',
                         security_enhancement_strategy='Active',
                         system_disk_category='cloud_ssd',
                         data_disks=None,
                         spot_strategy='NoSpot',
                         DeploymentSetId=None,
                         threadspercore=2):
        # TODO: The auto query should improve and move to higher level function.
        '''
    if unique_suffix:
      instance_list = list()
      for index in range(1, amount+1):
        instances = self.describe_instances(instance_name+str(index).zfill(3))
        if len(instances):
          instance_list.append(instances[0])
      if len(instance_list):   
        return instance_list
    else:
      instance_list = self.describe_instances(instance_name)
      if len(instance_list):
        return instance_list
    '''
        # https://help.aliyun.com/document_detail/102830.html?spm=a2c4g.11186623.6.583.43e03f35kmBN0a
        from aliyunsdkecs.request.v20140526.RunInstancesRequest import RunInstancesRequest
        request = RunInstancesRequest()
        request.set_accept_format('json')
        request.set_Amount(amount)
        request.set_InstanceName(instance_name)
        if host_name:
            request.set_HostName(host_name)
        # else:
        #   request.set_HostName(instance_name)
        request.set_UniqueSuffix(unique_suffix)
        request.set_InstanceType(instance_type)
        request.set_ImageId(image_id)
        request.set_VSwitchId(vswitch_id)

        if security_group_id:
            request.set_SecurityGroupId(security_group_id)
        else:
            security_group = self.create_security_group(vpc_id, instance_name + '-securyty_group',
                                                        default_authorize=True)
            request.set_SecurityGroupId(security_group.security_group_id())
        if password:
            request.set_Password(password)
        if keypair_name:
            request.set_KeyPairName(keypair_name)

        request.set_InternetChargeType(internet_charge_type)
        request.set_InternetMaxBandwidthOut(internet_max_bandwidth_out)
        request.set_IoOptimized(io_optimized)
        request.set_SecurityEnhancementStrategy(security_enhancement_strategy)
        request.set_SystemDiskSize(system_disk_size)
        request.set_SystemDiskCategory(system_disk_category)
        request.set_SpotStrategy(spot_strategy)
        if data_disks:
            ''' data disks format.
      [
        {
          "Size": "",
          "SnapshotId": "",
          "Category": "",
          "DiskName": "",
          "Description": "",
          "DeleteWithInstance": "true"
        }
      ]
      '''
            request.set_DataDisks(data_disks)
        if threadspercore:
            request.set_CpuOptionsThreadsPerCore(threadspercore)
            request.set_CpuOptionsCore(16)
        if system_disk_size:
            request.set_SystemDiskSize(system_disk_size)
        if system_disk_category:
            request.set_SystemDiskCategory(system_disk_category)
        if DeploymentSetId:
            request.set_DeploymentSetId(DeploymentSetId)

        response = self.action(request)
        time.sleep(1)
        instance_list = list()
        if response:
            response_json = json.loads(response)
            for instance_id in response_json['InstanceIdSets']['InstanceIdSet']:
                instance = Instance(instance_id)
                self.describe_instance_attribute(instance)
                instance_list.append(instance)

        return instance_list

    def stop_instance(self, instance: Instance):
        assert instance.is_running(), 'Status of instance should be running before stop instance.'
        from aliyunsdkecs.request.v20140526.StopInstanceRequest import StopInstanceRequest
        request = StopInstanceRequest()
        request.set_accept_format('json')
        request.set_InstanceId(instance())
        _ = self.action(request)
        time.sleep(1)
        self.describe_instance_attribute(instance)

    def start_instance(self, instance: Instance):
        assert instance.is_stopped(), 'Status of instance should be stopped before start instance.'
        from aliyunsdkecs.request.v20140526.StartInstanceRequest import StartInstanceRequest
        request = StartInstanceRequest()
        request.set_accept_format('json')
        request.set_InstanceId(instance())
        _ = self.action(request)
        self.describe_instance_attribute(instance)

    def reboot_instance(self, instance: Instance):
        assert instance.is_running(), 'Status of instance should be running before reboot instance.'
        from aliyunsdkecs.request.v20140526.RebootInstanceRequest import RebootInstanceRequest
        request = RebootInstanceRequest()
        request.set_accept_format('json')
        request.set_InstanceId(instance())
        _ = self.action(request)
        self.describe_instance_attribute(instance)

    def delete_instance(self, instance: Instance, force=False):
        if not force:
            assert instance.is_stopped(), 'Status of instance should be stopped before delete instance with non-force.'
        from aliyunsdkecs.request.v20140526.DeleteInstanceRequest import DeleteInstanceRequest
        request = DeleteInstanceRequest()
        request.set_accept_format('json')
        request.set_InstanceId(instance())
        request.set_Force(force)
        _ = self.action(request)
        self.describe_instance_attribute(instance)

    def wait_instance_running(self, instance: Instance):
        while not instance.is_running():
            time.sleep(1)
            self.describe_instance_attribute(instance)

    def wait_instance_stopped(self, instance: Instance):
        while not instance.is_stopped():
            time.sleep(1)
            self.describe_instance_attribute(instance)

    def modify_instance_name(self, instance: Instance, instance_name):
        from aliyunsdkecs.request.v20140526.ModifyInstanceAttributeRequest import ModifyInstanceAttributeRequest
        request = ModifyInstanceAttributeRequest()
        request.set_accept_format('json')
        request.set_InstanceId(instance())
        request.set_InstanceName(instance_name)
        _ = self.action(request)
        self.describe_instance_attribute(instance)

    # Aliyun security group openapi doc: https://help.aliyun.com/document_detail/104289.html?spm=a2c4g.11186623.6.632.135f637dCQBb2e
    def describe_security_groups(self, vpc_id=None, security_group_name=None, page_size=50):
        from aliyunsdkecs.request.v20140526.DescribeSecurityGroupsRequest import DescribeSecurityGroupsRequest
        request = DescribeSecurityGroupsRequest()
        request.set_accept_format('json')
        if vpc_id:
            request.set_VpcId(vpc_id)
        if security_group_name:
            request.set_SecurityGroupName(security_group_name)
        request.set_PageSize(page_size)

        response = self.action(request)
        response_json = json.loads(response)
        security_group_list = list()
        for security_group_json in response_json['SecurityGroups']['SecurityGroup']:
            security_group = SecurityGroup(attribute_json=security_group_json)
            security_group_list.append(security_group)

        return security_group_list

    def create_security_group(self, vpc_id,
                              security_group_name,
                              default_authorize=False):
        ''' The functionality of auto query should move to higher level code.
    security_group_list = self.describe_security_groups(vpc_id, security_group_name)
    if len(security_group_list):
      return security_group_list[0]
    '''
        from aliyunsdkecs.request.v20140526.CreateSecurityGroupRequest import CreateSecurityGroupRequest
        request = CreateSecurityGroupRequest()
        request.set_accept_format('json')
        request.set_VpcId(vpc_id)
        request.set_SecurityGroupName(security_group_name)
        response = self.action(request)
        security_group = SecurityGroup(attribute_json=json.loads(response))
        if default_authorize:
            self.authorize_security_group(security_group)
        return security_group

    def authorize_security_group(self, security_group: SecurityGroup,
                                 ip_protocol="TCP",
                                 port_range="22/22",
                                 source_cidr_ip="0.0.0.0/0",
                                 source_port_range=None):
        from aliyunsdkecs.request.v20140526.AuthorizeSecurityGroupRequest import AuthorizeSecurityGroupRequest
        request = AuthorizeSecurityGroupRequest()
        request.set_accept_format('json')
        request.set_SecurityGroupId(security_group())
        request.set_IpProtocol(ip_protocol)
        request.set_PortRange(port_range)
        request.set_SourceCidrIp(source_cidr_ip)
        if source_port_range:
            request.set_SourcePortRange(source_port_range)
        _ = self.action(request)

    def revoke_security_group(self, security_group: SecurityGroup,
                              ip_protocol="TCP",
                              port_range="22/22",
                              source_cidr_ip="0.0.0.0/0",
                              source_port_range=None):
        # remove ip from security group
        from aliyunsdkecs.request.v20140526.RevokeSecurityGroupRequest import RevokeSecurityGroupRequest
        request = RevokeSecurityGroupRequest()
        request.set_accept_format('json')
        request.set_SecurityGroupId(security_group())
        request.set_IpProtocol(ip_protocol)
        request.set_PortRange(port_range)
        request.set_SourceCidrIp(source_cidr_ip)
        if source_port_range:
            request.set_SourcePortRange(source_port_range)
        _ = self.action(request)

    def delete_security_group(self, security_group: SecurityGroup):
        from aliyunsdkecs.request.v20140526.DeleteSecurityGroupRequest import DeleteSecurityGroupRequest
        request = DeleteSecurityGroupRequest()
        request.set_accept_format('json')
        request.set_SecurityGroupId(security_group())

        _ = self.action(request)

    # Aliyun disk openapi https://help.aliyun.com/document_detail/104242.html?spm=a2c4g.11186623.3.3.5ce247f2ELfLaV
    def describe_disk_attribute(self, disk: Disk):
        from aliyunsdkecs.request.v20140526.ModifyDiskAttributeRequest import ModifyDiskAttributeRequest
        disk_list = self.describe_disks(disk_ids=[disk()])
        if len(disk_list):
            disk.update_attribute(json.dumps(disk_list[0].attribute))

    def describe_disks(self, disk_name=None,
                       disk_ids=None,
                       instance_id=None, page_size=50):
        from aliyunsdkecs.request.v20140526.DescribeDisksRequest import DescribeDisksRequest
        request = DescribeDisksRequest()
        # request.set_accept_format('json')
        if disk_name:
            request.set_DiskName(disk_name)
        if disk_ids:
            request.set_DiskIds(disk_ids)
        if instance_id:
            request.set_InstanceId(instance_id)
        request.set_PageSize(page_size)

        response = self.action(request)
        response_json = json.loads(response)
        disk_list = list()
        for disk_json in response_json['Disks']['Disk']:
            disk = Disk(attribute_json=disk_json)
            disk_list.append(disk)

        return disk_list

    def describe_ecsprices(self, resourceType='instance', instanceType=None, SystemDiskCategory
    ='cloud_ssd'):
        from aliyunsdkecs.request.v20140526.DescribePriceRequest import DescribePriceRequest
        request = DescribePriceRequest()
        request.set_ResourceType(resourceType)
        request.set_InstanceType(instanceType)
        request.set_SystemDiskCategory(SystemDiskCategory)
        request.set_SystemDiskSize(500)
        response = self.action(request)
        response_json = json.loads(response)
        price = response_json['PriceInfo']['Price']

        return price

    def create_disk(self,
                    zone_id,
                    snapshot_id=None,
                    disk_name=None,
                    disk_category='cloud_ssd',
                    size=500):
        from aliyunsdkecs.request.v20140526.CreateDiskRequest import CreateDiskRequest
        request = CreateDiskRequest()
        request.set_ZoneId(zone_id)
        if snapshot_id:
            request.set_SnapshotId(snapshot_id)
        if disk_name:
            request.set_DiskName(disk_name)
        request.set_DiskCategory(disk_category)
        request.set_Size(size)
        response = self.action(request)
        response_json = json.loads(response)
        disk = Disk(response_json['DiskId'])
        time.sleep(1)
        self.describe_disk_attribute(disk)
        return disk

    def attach_disk(self, disk: Disk,
                    instance: Disk,
                    delete_with_instance=False):
        assert disk.is_avaliable(), 'disk status should be avaliable before attach operation.'
        assert instance.is_running() or instance.is_stopped(), 'instance status should be running or stopped before attach operation.'
        from aliyunsdkecs.request.v20140526.AttachDiskRequest import AttachDiskRequest
        request = AttachDiskRequest()
        request.set_DiskId(disk())
        request.set_InstanceId(instance())
        request.set_DeleteWithInstance(delete_with_instance)
        _ = self.action(request)

    def detach_disk(self, disk: Disk,
                    instance: Instance):
        assert disk.is_avaliable(), 'disk status should be avaliable before detach operation.'
        assert instance.is_running() or instance.is_stopped(), 'instance status should be running or stopped before detach operation.'
        from aliyunsdkecs.request.v20140526.DetachDiskRequest import DetachDiskRequest
        request = DetachDiskRequest()
        request.set_DiskId(disk())
        request.set_InstanceId(instance())
        _ = self.action(request)

    def delete_disk(self, disk: Disk):
        assert disk.is_avaliable(), 'disk status should be avaliable before delete operation.'
        from aliyunsdkecs.request.v20140526.DeleteDiskRequest import DeleteDiskRequest
        request = DeleteDiskRequest()
        request.set_DiskId(disk())
        _ = self.action(request)

    def wait_disk_available(self, disk: Disk):
        while not disk.is_avaliable():
            time.sleep(1)
            self.describe_disk_attribute(disk)

    def wait_disk_in_use(self, disk: Disk):
        while not disk.is_in_use():
            time.sleep(1)
            self.describe_disk_attribute(disk)

    def wait_disk_attaching(self, disk: Disk):
        while disk.status() == "Attaching":
            time.sleep(1)
            self.describe_disk_attribute(disk)

    # aliyun snapshot openapi doc: https://help.aliyun.com/document_detail/104273.html?spm=a2c4g.11186623.6.619.45d647f2Z0iNec
    def describe_snapshot_attribute(self, snapshot: Snapshot):
        snapshot_list = self.describe_snapshots(snapshot_ids=[snapshot()])
        if len(snapshot_list):
            snapshot.update_attribute(json.dumps(snapshot_list[0].attribute))

    def describe_snapshots(self, disk_id=None,
                           snapshot_ids=None,
                           instance_id=None,
                           snapshot_name=None,
                           page_size=50):
        from aliyunsdkecs.request.v20140526.DescribeSnapshotsRequest import DescribeSnapshotsRequest
        request = DescribeSnapshotsRequest()
        if disk_id:
            request.set_DiskId(disk_id)
        if snapshot_ids:
            request.set_SnapshotIds(snapshot_ids)
        if snapshot_name:
            request.set_SnapshotName(snapshot_name)
        if instance_id:
            request.set_InstanceId(instance_id)
        request.set_PageSize(page_size)
        response = self.action(request)
        snapshot_list = list()
        if response:
            response_json = json.loads(response)
            for snapshot_json in response_json['Snapshots']['Snapshot']:
                snapshot = Snapshot(attribute_json=snapshot_json)
                snapshot_list.append(snapshot)
        return snapshot_list

    def create_snapshot(self, disk: Disk,
                        snapshot_name=None):
        assert disk.is_in_use(), 'Status of disk should be in use before create snapshot.'
        instance = Instance(id=disk.instance_id())
        self.describe_disk_attribute(instance)
        assert instance.is_running() or instance.is_stopped(), 'Status of instance should be running or stopped before create snapshot.'
        from aliyunsdkecs.request.v20140526.CreateSnapshotRequest import CreateSnapshotRequest
        request = CreateSnapshotRequest()
        request.set_DiskId(disk())
        if snapshot_name:
            request.set_SnapshotName(snapshot_name)
        response = self.action(request)
        response_json = json.loads(response)
        snapshot = Snapshot(response_json['SnapshotId'])
        time.sleep(1)
        self.describe_snapshot_attribute(snapshot)
        return snapshot

    def delete_snapshot(self, snapshot: Snapshot):
        from aliyunsdkecs.request.v20140526.DeleteSnapshotRequest import DeleteSnapshotRequest
        request = DeleteSnapshotRequest()
        request.set_SnapshotId(snapshot())
        _ = self.action(request)

    def wait_snapshot_accomplished(self, snapshot: Snapshot):
        time.sleep(1)
        self.describe_snapshot_attribute(snapshot)
        if snapshot.is_progressing():
            while not snapshot.is_accomplished() and not snapshot.is_faied():
                if snapshot.process() < '20':
                    time.sleep(5)
                    self.describe_snapshot_attribute(snapshot)
                else:
                    time.sleep(snapshot.remain_time())
                    self.describe_snapshot_attribute(snapshot)

    def describe_key_pairs(self, key_pair_name=None, key_pair_finger_print=None, page_size=50):
        from aliyunsdkecs.request.v20140526.DescribeKeyPairsRequest import DescribeKeyPairsRequest
        request = DescribeKeyPairsRequest()
        if key_pair_name:
            request.set_KeyPairName(key_pair_name)
        if key_pair_finger_print:
            request.set_KeyPairFingerPrint(key_pair_finger_print)
        request.set_PageSize(page_size)
        response = self.action(request)
        key_pair_list = list()
        if response:
            response_json = json.loads(response)
            for key_pair_json in response_json['KeyPairs']['KeyPair']:
                key_pair = KeyPair(attribute_json=key_pair_json)
                key_pair_list.append(key_pair)

        return key_pair_list

    def create_key_pair(self, key_pair_name, save_path=None, resource_group_id=None):
        from aliyunsdkecs.request.v20140526.CreateKeyPairRequest import CreateKeyPairRequest
        request = CreateKeyPairRequest()
        request.set_KeyPairName(key_pair_name)
        if resource_group_id:
            request.set_ResourceGroupId(resource_group_id)
        response = self.action(request)
        if response:
            private_key_str = json.loads(response)['PrivateKeyBody']
            if save_path:
                with open(save_path, 'w') as f:
                    f.write(private_key_str)
                    print('private save to: ' + save_path)
            return private_key_str

    def attach_key_pair(self, instance_ids: list, key_pair_name):
        from aliyunsdkecs.request.v20140526.AttachKeyPairRequest import AttachKeyPairRequest
        request = AttachKeyPairRequest()
        request.set_InstanceIds(instance_ids)
        request.set_KeyPairName(key_pair_name)
        response = self.action(request)
        response_json = json.loads(response)
        for result_json in response_json['Results']:
            assert result_json['Success'] == 'true', 'Instance {} attach keypair failed.'.format(
                response_json['InstanceId'])

    def detach_key_pair(self, instance_ids: list, key_pair_name):
        from aliyunsdkecs.request.v20140526.DetachKeyPairRequest import DetachKeyPairRequest
        request = DetachKeyPairRequest()
        request.set_InstanceIds(instance_ids)
        request.set_KeyPairName(key_pair_name)
        response = self.action(request)
        response_json = json.loads(response)
        for result_json in response_json['Results']['Result']:
            assert result_json['Success'] == True, 'Instance {} attach keypair failed.'.format(
                response_json['InstanceId'])

    def delete_key_pairs(self, key_pair_names: list):
        from aliyunsdkecs.request.v20140526.DeleteKeyPairsRequest import DeleteKeyPairsRequest
        request = DeleteKeyPairsRequest()
        request.set_KeyPairNames(key_pair_names)
        _ = self.action(request)

    def create_deploymentset(self, DeploymentSetName, GroupCount):
        from aliyunsdkecs.request.v20140526.CreateDeploymentSetRequest import CreateDeploymentSetRequest
        request = CreateDeploymentSetRequest()
        request.set_DeploymentSetName(DeploymentSetName)
        request.set_GroupCount(GroupCount)

        response = self.action(request)
        if response:
            DeploymentSetId = json.loads(response)['DeploymentSetId']
        return DeploymentSetId

    def describe_deploymentSets(self, DeploymentSetIds):
        from aliyunsdkecs.request.v20140526.DescribeDeploymentSetsRequest import DescribeDeploymentSetsRequest
        request = DescribeDeploymentSetsRequest()
        request.set_DeploymentSetIds(DeploymentSetIds)

        response = self.action(request)
        return response
