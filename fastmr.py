import os
import sys
from configparser import ConfigParser

import mracc


def main():
    try:
        conf_path = os.path.abspath(sys.argv[1])
    except:
        print("Please input an config, like this :  python3 fastmr.py config.example.ini")
    config = ConfigParser()
    config.read(conf_path, encoding='UTF-8')
    skip_setup = config.getboolean('cmd', 'skip_setup')
    mracc.def_conf(conf_path)
    # 创建集群
    engine = config['engine']['model']
    if engine == 'CDT':
        job = mracc.create_cluster()
    if engine == 'DT':
        job = mracc.control_cluster()
    # 部署环境
    if not skip_setup:
        mracc.setup_pkg(job)
        mracc.setup_env(job)
    # 运行测试
    mracc.run_tpc(job)

if __name__ == '__main__':
    main()