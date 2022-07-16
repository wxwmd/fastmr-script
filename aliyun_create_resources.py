#!/usr/bin/env python
#
# Creates resources
# This script creates VPC/security group/keypair if not already present

import os
import sys
import time
import argparse

from ncluster import aliyun_util as u
from ncluster import util
from ncluster import ncluster_globals


DRYRUN = False
DEBUG = True

# Names of Amazon resources that are created. These settings are fixed across
# all runs, and correspond to resources created once per user per region.

PUBLIC_TCP_RANGES = [
  # ipython notebook ports
  (8888, 8899),
  # redis port
  6379,
  # tensorboard ports
  (6006, 6016)
]

SSH_PORT = 22

PUBLIC_UDP_RANGES = [(60000, 61000)]  # mosh ports
DEFAULT_SUPPORT_INSTANCE = 'ecs.g5'
DEFAULT_SUPPORT_DISK = 'cloud_ssd'


# TODO: this creates a custom VPC, but we are using default VPC, so have two security groups
# once we are sure we don't need custom VPC, can get rid of extra VPC creation

def network_setup(support_instance_type=None, support_disk_type=None):
  """Creates VPC if it doesn't already exists, configures it for public
  internet access, returns vpc, subnet, security_group"""

  ecs_client = u.get_ecs_client()
  vpc_client = u.get_vpc_client()
  existing_vpcs = u.get_vpc_dict()
  zones = u.get_zones()

  # create VPC from scratch. Remove this if default VPC works well enough.
  vpc_name = u.get_vpc_name()
  if u.get_vpc_name() in existing_vpcs:
    print("Reusing VPC " + vpc_name)
    vpc = existing_vpcs[vpc_name]

  else:
    print("Creating VPC " + vpc_name)
    vpc = vpc_client.create_vpc(vpc_name=vpc_name, cidr_block='192.168.0.0/16')

  vswitches = u.get_vswitch_dict(vpc)
  vswitch_name = u.get_vswitch_name()
  if vswitch_name in vswitches:
    print("Reusing vswitch " + vswitch_name)
    vswitch = vswitches[vswitch_name]
  else:
    assert len(zones) <= 16  # for cidr/20 to fit into cidr/16
    ip = 0
    for zone in zones:
      cidr_block = '192.168.%d.0/20' % (ip,)
      ip += 16
      print("Creating vswitch of cidr_block %s in zone %s" % (cidr_block, zone.zone_id()))
      zone_id = zone.zone_id()
      sub_vswitch_name = f'{vpc_name}-{zone_id[-1]}'
      try:
        vswitch = vpc_client.create_vswitch(vpc, cidr_block=cidr_block, vswitch_name=sub_vswitch_name, zone_id=zone_id)
        time.sleep(1)
        print(f'Create vswitch {vswitch.vswitch_name()} succeed.')
      except Exception as _:
        print(f'{zone.zone_id()} can\'t create vswitch')


  existing_security_groups = u.get_security_group_dict()
  #security_group_name = u.get_security_group_name()
  security_group_name = u.get_security_group_id()
  
  if security_group_name in existing_security_groups:
    print("Reusing security group " + security_group_name)
    security_group = existing_security_groups[security_group_name]
    assert security_group.vpc_id() == vpc(), f"Found security group {security_group} " \
                                            f"attached to {security_group.vpc_id()} but expected {vpc()}"
  else:
    print("Creating security group " + security_group_name)
    security_group = ecs_client.create_security_group(vpc_id=vpc(),
      security_group_name=security_group_name)
    # allow ICMP access for public ping
    ecs_client.authorize_security_group(security_group, 
                                        ip_protocol='ICMP',
                                        port_range='-1/-1',
                                        source_cidr_ip='0.0.0.0/0')


    # other public ports for '0.0.0.0/0'
    for port in PUBLIC_TCP_RANGES:
      if util.is_iterable(port):
        assert len(port) == 2
        from_port, to_port = port
      else:
        from_port, to_port = port, port
      to_port_range = str(from_port) + '/' + str(to_port)
      ecs_client.authorize_security_group(security_group,
                                          ip_protocol="TCP",
                                          port_range=to_port_range,
                                          source_cidr_ip="0.0.0.0/0")

    for port in PUBLIC_UDP_RANGES:
      if util.is_iterable(port):
        assert len(port) == 2
        from_port, to_port = port
      else:
        from_port, to_port = port, port
      to_port_range = str(from_port) + '/' + str(to_port)
      ecs_client.authorize_security_group(security_group,
                                          ip_protocol="UDP",
                                          port_range=to_port_range,
                                          source_cidr_ip="0.0.0.0/0")
  
  # open public ports
  #addip(ecs_client, security_group)

  return vpc

def addip(ecs_client=None, security_group=None, ip=None):
  # open public ports
  # always include SSH port which is required for basic functionality
  import urllib.request
  if not ecs_client:
    ecs_client = u.get_ecs_client()
  if not security_group:
    existing_security_groups = u.get_security_group_dict()
    #security_group_name = u.get_security_group_name()
    security_group_name = u.get_security_group_id()
    security_group = existing_security_groups[security_group_name]
  external_ip = ip
  if not external_ip:
    external_ip = urllib.request.urlopen('http://ip.cip.cc').read().decode('utf8').strip('\n')
  authorize_ip_range = f'{external_ip}/24'
  ecs_client.authorize_security_group(security_group,
                                      ip_protocol="TCP",
                                      source_cidr_ip=authorize_ip_range)
 

def keypair_setup():
  """Creates keypair if necessary, saves private key locally, returns contents
  of private key file."""

  os.system('mkdir -p ' + u.PRIVATE_KEY_LOCATION)

  keypair_name = u.get_keypair_name()
  keypair = u.get_keypair(keypair_name)
  keypair_fn = u.get_keypair_fn()
  
  if keypair:
    print("Reusing keypair " + keypair_name)
    # check that local pem file exists and is readable
    assert os.path.exists(
      keypair_fn), "Keypair %s exists, but corresponding .pem file %s is not found, delete keypair %s through console and run again to recreate keypair/.pem together" % (
      keypair_name, keypair_fn, keypair_name)
    keypair_contents = open(keypair_fn).read()
    assert len(keypair_contents) > 0
  else:
    print("Creating keypair " + keypair_name)
    client = u.get_ecs_client()
    assert not os.path.exists(
      keypair_fn), "previous keypair exists, delete it with 'sudo rm %s' and also delete corresponding keypair through console" % (
      keypair_fn)
    keypair_private_key_str = client.create_key_pair(key_pair_name=keypair_name)

    open(keypair_fn, 'w').write(keypair_private_key_str)
    os.system('chmod 400 ' + keypair_fn)

  return keypair


def create_resources(disable_nas=False):
  print(f"Creating {u.get_prefix()} resources in region {u.get_region()}")
  vpc = network_setup()
  keypair_setup()  # saves private key locally to keypair_fn

  # create NAS
  if not disable_nas:
    nass = u.get_nas_dict()
    nas_name = u.get_nas_name()
    nas = nass.get(nas_name, None)
    if not nas:
      print("Creating NAS " + nas_name)
      nas = u.create_nas(nas_name)
    else:
      print("Reusing NAS " + nas_name)
    

    nas_client = u.get_nas_client()
    current_zone = u.get_zone()
    if current_zone:
      vswitch = u.get_vswitch()
      mount_target_dict = u.get_mount_target_dict()
      if len(mount_target_dict) < 2:
        print("Create new Mount target")
        nas_client.create_mount_target(file_system_id=nas(), vpc_id=vpc(), vswitch_id=vswitch())
        # time.sleep(1)
      else:
        print("!!!!! Warninig: Mount targets limit is 2, Skip mount NAS for current Job/Task.")
        print("!!!!! Warninig: If you wan't to modify the mount taget, please check the usage of current NAS carefully.")
        ncluster_globals.set_should_disable_nas(True)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--disable-nas', type=bool, default=False,
                    help="If disable-nas is true, will skip the NAS creatation and launch.")
  args = parser.parse_args()
  create_resources(disable_nas=args.disable_nas)
