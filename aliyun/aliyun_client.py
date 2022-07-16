import traceback
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException

class AliyunClient(object):
  client: AcsClient
  
  def __init__(self, access_key_id, access_key_secret, region_id):
    self.client = AcsClient(ak=access_key_id,
                              secret=access_key_secret,
                              region_id=region_id)

  def action(self, request)->str:
    response = self.client.do_action_with_exception(request)
    return str(response, encoding='utf-8')
    '''
    try:
      response = self.client.do_action_with_exception(request)
    except ClientException as e:
      print(f"{type(request).__name__} WARNING: Client Code: {e.error_code}, Message: {e.message}")
      return str()
    except ServerException as e:
      print(f"{type(request).__name__} WARNING: Server Code: {e.error_code}, Message: {e.message}")
      return str()
    except Exception:
      print(f"{type(request).__name__} Unhandled error")
      print(traceback.format_exc())
      return str()
    else:
      return str(response, encoding='utf-8')
    '''