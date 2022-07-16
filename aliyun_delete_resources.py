#!/usr/bin/env python
#
# Deletes resources

import sys
import os
import argparse
import time

import aliyun_util as u
import util

NAS_NAME = u.get_prefix()
VPC_NAME = u.get_prefix()
SECURITY_GROUP_NAME = u.get_prefix()
KEYPAIR_NAME = u.get_keypair_name()



def delete_nas():
  nass = u.get_nas_dict()
  nas = nass.get(NAS_NAME, None)
  nas_client = u.get_nas_client()
  if nas:
    try:
      # delete mount targets first
      print("About to delete %s (%s)" % (nas(), NAS_NAME))
      mount_target_list = nas_client.describe_mount_targets(file_system_id=nas())
      for mount_target in mount_target_list:
        nas_client.delete_mount_target(nas(), mount_target)

      sys.stdout.write('Deleting NAS %s (%s)... ' % (nas(), NAS_NAME))
      sys.stdout.flush()
      nas_client.delete_file_system(nas)

    except Exception as e:
      sys.stdout.write(f'failed with {e}\n')
      util.log_error(str(e) + '\n')


def delete_network():
  existing_vpcs = u.get_vpc_dict()
  if VPC_NAME in existing_vpcs:
    vpc = existing_vpcs[VPC_NAME]
    print("Deleting VPC %s (%s) subresources:" % (VPC_NAME, vpc()))
    # delete security group
    ecs_client = u.get_ecs_client()
    security_group_list = ecs_client.describe_security_groups(vpc_id=vpc())
    for security_group in security_group_list:
      ecs_client.delete_security_group(security_group)
    time.sleep(2)
    # delete vpc.
    vpc_client = u.get_vpc_client()
    vpc_client.delete_vpc(vpc)


def delete_keypair():
  ecs_client = u.get_ecs_client()
  keypairs = u.get_keypair_dict()
  keypair = keypairs.get(KEYPAIR_NAME, None)
  if keypair:
    try:
      sys.stdout.write("Deleting keypair %s (%s) ... " % (keypair.key_pair_name(),
                                                          KEYPAIR_NAME))
      ecs_client.delete_key_pairs(key_pair_names=[keypair.key_pair_name()])
    except Exception as e:
      sys.stdout.write('failed\n')
      util.log_error(str(e) + '\n')

  keypair_fn = u.get_keypair_fn()
  if os.path.exists(keypair_fn):
    print("Deleting local keypair file %s" % (keypair_fn,))
    os.system('rm -f ' + keypair_fn)


def delete_resources(args):
  region = os.environ['ALIYUN_DEFAULT_REGION']

  resource = u.get_prefix()
  print(f"Deleting {resource} resources in region {region}")
  print(f"Make sure {resource} instances are terminated or this will fail.")

  if 'efs' in args.kind or 'all' in args.kind:
    if NAS_NAME == u.DEFAULT_PREFIX and not args.force_delete_nas:
      # this is default EFS, likely has stuff, require extra flag to delete it
      print("default NAS has useful stuff in it, not deleting it. Use force-delete-nas "
            "flag to force")
    else:
      delete_nas()
  if 'network' in args.kind or 'all' in args.kind:
    delete_network()
  if 'keypair' in args.kind or 'all' in args.kind:
    delete_keypair()


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--kind', type=str, default='all',
                    help="which resources to delete, all/network/keypair/nas")
  parser.add_argument('--force-delete-nas', action='store_true',
                    help="force deleting main NAS")
  args = parser.parse_args()
  delete_resources(args)
