""" The Aliyun OpenAPI wrapper."""
'''
os env used to configure the Session
ALIYUN_ACCESS_KEY_ID: the access_key for aliyun account.
ALIYUN_ACCESS_KEY_SECRET: The access_key_secret for aliyun account.
ALIYUN_DEFAULT_REGION: The default region used at aliyun account.
'''

import os
from .ecs_client import EcsClient
from .vpc_client import VpcClient
from .nas_client import NasClient

class Session(object):
  access_key_id: str
  access_key_secret: str
  default_region: str

  def __init__(self, access_key_id=None, access_key_secret=None, default_region=None):
    if access_key_id:
      self.access_key_id = access_key_id
    else:
      self.access_key_id = os.environ['ALIYUN_ACCESS_KEY_ID']
    if access_key_secret:
      self.access_key_secret = access_key_secret
    else:
      self.access_key_secret = os.environ['ALIYUN_ACCESS_KEY_SECRET']
    if default_region:
      self.default_region = default_region
    else:
      self.default_region = os.environ['ALIYUN_DEFAULT_REGION']
  
  def get_ecs_client(self):
    return EcsClient(self.access_key_id, self.access_key_secret, self.default_region)

  def get_vpc_client(self):
    return VpcClient(self.access_key_id, self.access_key_secret, self.default_region)
  
  def get_nas_client(self):
    return NasClient(self.access_key_id, self.access_key_secret, self.default_region)