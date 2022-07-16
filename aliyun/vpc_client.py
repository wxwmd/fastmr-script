import json
import time
from .aliyun_object import Vpc, VSwitch
from .aliyun_client import AliyunClient

class VpcClient(AliyunClient):

  def __init__(self, access_key_id, access_key_secret, region_id):
    super().__init__(access_key_id, access_key_secret, region_id)
  
  def describe_vpc_attribute(self, vpc):
    from aliyunsdkvpc.request.v20160428.DescribeVpcAttributeRequest import DescribeVpcAttributeRequest
    request = DescribeVpcAttributeRequest()
    request.set_accept_format('json')
    request.set_VpcId(vpc())
    response = self.action(request)
    assert response, 'describe vpc attribute failed.'
    vpc.update_attribute(response)
  
  def describe_vpcs(self, vpc_id=None, vpc_name=None):
    from aliyunsdkvpc.request.v20160428.DescribeVpcsRequest import DescribeVpcsRequest
    request = DescribeVpcsRequest()
    request.set_accept_format('json')
    if vpc_id:
     request.set_VpcId(vpc_id)
    if vpc_name:
      request.set_VpcName(vpc_name)

    response = self.action(request)
    vpcs = list()
    if response:     
      response_json = json.loads(response)
      for json_str in response_json['Vpcs']['Vpc']:
        vpc_instance = Vpc(attribute_json=json_str)
        vpcs.append(vpc_instance)
  
    return vpcs

  def create_vpc(self, vpc_name=None, zone_id=None, cidr_block='192.168.0.0/16', create_vswitch=False)->Vpc:
    from aliyunsdkvpc.request.v20160428.CreateVpcRequest import CreateVpcRequest
    # The functionality of auto query vpc should move to higher level class.
    '''
    if vpc_name:
      vpc_list = self.describe_vpcs(vpc_name=vpc_name)
      if vpc_list:
        return vpc_list[0]
    '''
    request = CreateVpcRequest()
    request.set_accept_format('json')
    request.set_CidrBlock(cidr_block)
    if vpc_name:
      request.set_VpcName(vpc_name)
    response = self.action(request)
    response_json = json.loads(response)
    vpc = Vpc(response_json['VpcId'])
    self.describe_vpc_attribute(vpc)
    self.wait_vpc_avaliable(vpc)
    
    if create_vswitch:
      if vpc_name:
        vswitch_name = vpc_name
      vswitch = self.create_vswitch(vpc, vswitch_name, zone_id)
      self.wait_vswitch_avaliable(vswitch)
      self.describe_vpc_attribute(vpc)
    return vpc
  
  def delete_vpc(self, vpc:Vpc):
    for vswitch_id in vpc.vswitch_ids():
      self.delete_vswitch(VSwitch(vswitch_id))
    
    # sleep for rounte delete complete
    if len(vpc.vswitch_ids()):
      time.sleep(5)
    from aliyunsdkvpc.request.v20160428.DeleteVpcRequest import DeleteVpcRequest
    request = DeleteVpcRequest()
    request.set_accept_format('json')
    request.set_VpcId(vpc())
    _ = self.action(request)
  
  def wait_vpc_avaliable(self, vpc:Vpc):
    while vpc.status() != 'Available':
      time.sleep(1)
      self.describe_vpc_attribute(vpc)
  
  # vswitch
  def describe_vswitch_attribute(self, vswitch:VSwitch):
    from aliyunsdkvpc.request.v20160428.DescribeVSwitchAttributesRequest import DescribeVSwitchAttributesRequest
    request = DescribeVSwitchAttributesRequest()
    request.set_accept_format('json')
    request.set_VSwitchId(vswitch())

    response = self.action(request)
    vswitch.update_attribute(response)
  
  def describe_vswitches(self, vpc_id=None, vswitch_name=None):
    from aliyunsdkvpc.request.v20160428.DescribeVSwitchesRequest import DescribeVSwitchesRequest
    request = DescribeVSwitchesRequest()
    request.set_accept_format('json')
    if vpc_id:
      request.set_VpcId(vpc_id)
    if vswitch_name:
      request.set_VSwitchName(vswitch_name)

    response = self.action(request)
    vswitch_list = list()
    if response:
      response_json = json.loads(response)
      for vswitch_json in response_json['VSwitches']['VSwitch']:
        vswitch = VSwitch(attribute_json=vswitch_json)
        vswitch_list.append(vswitch)
    
    return vswitch_list

  def create_vswitch(self, vpc:Vpc, cidr_block=None, vswitch_name=None, zone_id=None):
    from aliyunsdkvpc.request.v20160428.CreateVSwitchRequest import CreateVSwitchRequest
    # 
    '''
    vswitch_list = self.describe_vswitches(vpc(), vswitch_name)
    if vswitch_list:
      return vswitch_list[0]
    '''

    request = CreateVSwitchRequest()
    request.set_accept_format('json')
    if zone_id:
      request.set_ZoneId(zone_id)
    else:
      default_zone_id = self.describe_zones()[0]
      request.set_ZoneId(default_zone_id)
    
    request.set_VpcId(vpc.vpc_id())
    if cidr_block:
      request.set_CidrBlock(cidr_block)
    else:
      request.set_CidrBlock(vpc.cidr_block())
    if vswitch_name:
      request.set_VSwitchName(vswitch_name)

    response = self.action(request)
    resonse_json = json.loads(response)
    vswitch = VSwitch(resonse_json['VSwitchId'])
    self.describe_vswitch_attribute(vswitch)
    return vswitch

  def delete_vswitch(self, vswitch:VSwitch):
    from aliyunsdkvpc.request.v20160428.DeleteVSwitchRequest import DeleteVSwitchRequest
    request = DeleteVSwitchRequest()
    request.set_accept_format('json')
    request.set_VSwitchId(vswitch())
    _ = self.action(request)
  
  def wait_vswitch_avaliable(self, vswitch:VSwitch):
    while vswitch.status() != 'Available':
      self.describe_vswitch_attribute(vswitch)
  
  def describe_zones(self):
    from aliyunsdkvpc.request.v20160428.DescribeZonesRequest import DescribeZonesRequest
    request = DescribeZonesRequest()
    request.set_accept_format('json')

    response = self.action(request)
    response_json = json.loads(response)
    zone_id_list = list()
    for zone_json in response_json['Zones']['Zone']:
      zone_id_list.append(zone_json['ZoneId'])
    return zone_id_list
