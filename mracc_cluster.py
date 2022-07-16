#!/usr/bin/env python

import fileinput
import math
import os
import shutil
import time
import xml.etree.ElementTree as ET
from configparser import ConfigParser

import ncluster


def def_conf(conf_path):
    global CONF_PATH, FASTMR_PATH, CLUSTER_NAME

    CONF_PATH = conf_path
    FASTMR_PATH = os.path.abspath(f'{conf_path}/../..')
    config = ConfigParser()
    config.read(CONF_PATH, encoding='UTF-8')
    CLUSTER_NAME = config['ncluster']['clustername']


def control_cluster():
    ncluster.set_backend('fastmr')

    # config = "config.ini"
    config = ConfigParser()

    config.read(CONF_PATH, encoding='UTF-8')

    num_tasks = config.getint('ncluster', 'machines')
    copy_conf()
    tasks_message = [[config['master']['public_ip'],
                      config['master']['usr'],
                      config['master']['passwd']]]

    for i in range(num_tasks - 1):
        tasks_message.append([config[f'worker{i}']['public_ip'],
                              config[f'worker{i}']['usr'],
                              config[f'worker{i}']['passwd']])

    job = ncluster.make_job(name=CLUSTER_NAME,
                            num_tasks=num_tasks,
                            tasks_message=tasks_message)
    return job


def copy_conf():
    if os.path.exists(FASTMR_PATH + "/target/" + CLUSTER_NAME):
        shutil.rmtree(FASTMR_PATH + "/target/" + CLUSTER_NAME)
    shutil.copytree(f"{FASTMR_PATH}/trans", FASTMR_PATH + "/target/" + CLUSTER_NAME)


def create_cluster():
    """
    Args:
    path: config path
    Returns:
    Job
    """

    ncluster.set_backend('aliyun')

    # config = "config.ini"
    config = ConfigParser()

    config.read(CONF_PATH, encoding='UTF-8')

    IMAGE_NAME = config['ncluster']['image_name']

    # 1. Create infrastructure
    supported_regions = ['cn-shenzhen', 'cn-wulanchabu', 'cn-huhehaote', 'cn-zhangjiakou', 'cn-shanghai', 'cn-hangzhou',
                         'cn-beijing']
    assert ncluster.get_region() in supported_regions, f"required AMI {IMAGE_NAME} has only been made available in regions {supported_regions}, but your current region is {ncluster.get_region()} (set $ALYUN_DEFAULT_REGION)"

    # cluster

    if config.has_option('ncluster', 'instancename'):
        instancename = config['ncluster']['instancename']
    else:
        instancename = CLUSTER_NAME

    INSTANCE_TYPE = config['ncluster']['instance_type']
    machines = config.getint('ncluster', 'machines')
    system_disk_size = config['ncluster']['system_disk_size']
    if config.has_option('ncluster', 'system_disk_category'):
        system_disk_category = config['ncluster']['system_disk_category']
    vpc_name = config['ncluster']['vpc_name']
    skip_setup = config.getboolean('ncluster', 'skip_setup')
    threadsPerCore = None
    if (config.has_option('ncluster', 'threadsPerCore')):
        threadsPerCore = config['ncluster']['threadsPerCore']
    cloud_data_disk_size = None
    if (config.has_option('ncluster', 'cloud_data_disk_size')):
        cloud_data_disk_size = config['ncluster']['cloud_data_disk_size']
    cloud_disk_num = None
    if (config.has_option('ncluster', 'cloud_disk_num')):
        cloud_disk_num = config['ncluster']['cloud_disk_num']
    cloud_disk_type = 'PL1'
    if (config.has_option('ncluster', 'cloud_disk_type')):
        cloud_disk_type = config['ncluster']['cloud_disk_type']
    DeploymentSetId = None
    if config.has_option('ncluster', 'DeploymentSetId'):
        DeploymentSetId = config['ncluster']['DeploymentSetId']

    # copy conf file

    start_time = time.time()
    copy_conf()
    cinfofile = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/cluster.info"
    with open(cinfofile, 'w+') as f:
        f.write("#MRACC Bigdata cluster info. cluster name: " + CLUSTER_NAME + "   " + start_time.__str__() + "\n")
        f.close()
    os.system(f"echo ------price infos: >> {cinfofile}")
    os.system(f"cat resource/ecsprices.csv | grep '{INSTANCE_TYPE}' >> {cinfofile}")
    os.system(f"echo ------instance infos: >> {cinfofile}")
    os.system(f"cat resource/ecsresource.csv | grep '{INSTANCE_TYPE}' >> {cinfofile}")

    # 创建集群

    if cloud_data_disk_size is not None:
        job = ncluster.make_job(cname=CLUSTER_NAME,
                                name=instancename,
                                run_name=CLUSTER_NAME + str(machines),
                                num_tasks=machines,
                                instance_type=INSTANCE_TYPE,
                                machines=machines,
                                vpc_name=vpc_name,
                                image_name=IMAGE_NAME,
                                system_disk_category=system_disk_category,
                                skip_setup=skip_setup,
                                threadsPerCore=threadsPerCore,
                                cloud_data_disk_size=cloud_data_disk_size,
                                cloud_disk_num=cloud_disk_num,
                                DeploymentSetId=DeploymentSetId,
                                cloud_disk_type=cloud_disk_type
                                )
    else:
        job = ncluster.make_job(cname=CLUSTER_NAME,
                                name=instancename,
                                run_name=CLUSTER_NAME + str(machines),
                                num_tasks=machines,
                                instance_type=INSTANCE_TYPE,
                                machines=machines,
                                vpc_name=vpc_name,
                                system_disk_category=system_disk_category,
                                DeploymentSetId=DeploymentSetId,
                                threadsPerCore=threadsPerCore,
                                image_name=IMAGE_NAME
                                )
    print(f"{time.time()} : mrcluster {CLUSTER_NAME} have {len(job.tasks)} workers inited")
    return job


def align_guava(master):
    # 只考虑hadoop中guava版本高于hive的情况
    hadoop_guava = master.run('find $HADOOP_HOME/share/hadoop/common/lib/ -name "guava*"')[0:-1]
    hive_guava = master.run('find $HIVE_HOME/lib/ -name "guava*"')[0:-1]
    print(f"{hive_guava}")
    print(f"mv {hive_guava} {hive_guava}.bak")
    print(f"mv {hadoop_guava} $HIVE_HOME/lib/")
    if hive_guava != "" and hadoop_guava != "":
        master.run(f"mv {hive_guava} {hive_guava}.bak")
        master.run(f"mv {hadoop_guava} $HIVE_HOME/lib/")
    else:
        print("miss guava.jar in hadoop or hive")
    return


def config_mysql(master):
    master.run("service mysqld restart")
    prefix = "mysql -uroot -D mysql -e"
    master.run(f"""{prefix} "create user 'hive'@'localhost' identified by '123456';" """)
    master.run(f"""{prefix} "grant all privileges on *.* to 'hive'@'localhost';"  """)
    master.run(f"""{prefix} "create user 'hive'@'%' identified by '123456';" """)
    master.run(f"""{prefix} "grant all privileges on *.* to 'hive'@'%';" """)
    master.run(f"""{prefix} "flush privileges;" """)
    master.run("service mysqld restart")
    return


def mysql_connect_jar(master):
    result = master.run("mysql --version").split()
    version = result[2]
    if master.exists(f"$HIVE_HOME/lib/mysql-connector-java-{version}.jar"):
        return
    else:
        try:
            master.run(
                f"wget -P /root https://downloads.mysql.com/archives/get/p/3/file/mysql-connector-java-{version}.tar.gz")
        except Exception as e:
            print("down mysql driver failed,please remove mysql in master and restart mysql")
        master.run(f"cd /root && tar -zxvf /root/mysql-connector-java-{version}.tar.gz")
        master.run(f"cp /root/mysql-connector-java-{version}/mysql-connector-java-{version}.jar $HIVE_HOME/lib/")
        master.run(f"cp /root/mysql-connector-java-{version}/mysql-connector-java-{version}.jar $SPARK_HOME/jars/")
    return


def setup_pkg(job):
    config = ConfigParser()
    config.read(CONF_PATH, encoding='UTF-8')

    env_str = ""
    # set up java

    job.setup('jdk-8u321-linux-x64', pkg_format='.rpm')

    env_str += f"export JAVA_HOME=/usr/java/jdk1.8.0_321-amd64 \n"

    # set up hadoop
    hadoop_version = config['hadoop']['version']

    job.setup(f'hadoop-{hadoop_version}')

    env_str += f"export HADOOP_HOME=/opt/hadoop-{hadoop_version} \n"
    env_str += f"export HADOOP_CONF_DIR=/opt/hadoop-{hadoop_version}/etc/hadoop \n"

    # set up spark
    spark_version = config['spark']['version']
    job.setup(f'spark-{spark_version}', pkg_format='.tgz')
    env_str += f"export SPARK_HOME=/opt/spark-{spark_version} \n"

    # set up hive
    hive_version = config['hive']['version']
    job.setup(f'apache-hive-{hive_version}')
    env_str += f"export HIVE_HOME=/opt/apache-hive-{hive_version} \n"

    env_str += f"export PATH=$PATH:$HADOOP_HOME/bin:$SPARK_HOME/bin:$HIVE_HOME/bin \n"

    # hive thing and mysql metastore
    job.upload(f"{FASTMR_PATH}/target/{CLUSTER_NAME}/config/hive/hive-site.xml",
               f"/opt/apache-hive-{hive_version}/conf/hive-site.xml")
    job.upload(f"{FASTMR_PATH}/target/{CLUSTER_NAME}/config/hive/hive-site.xml",
               f"/opt/spark-{spark_version}/conf/hive-site.xml")

    job.setup('TPC')

    job.setup('tpcds-kit', path='/root')

    conf_env(job, env_str)


def setup_env(job):
    init_disk(job)

    conf_hadoop(job)

    conf_spark(job)

    start_cluster(job.tasks[0], job)

    # start_flame(job.tasks[0])

    show_info(job.tasks[0])


def show_info(master):
    infofile = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/cluster.info"
    with open(infofile, 'a+') as f:
        # f.write("------host infos:\n")
        # f.write(hostsstr)
        # f.write("------worker infos:\n")
        # f.write(slavesstr)
        # f.write("------cluster infos:\n")
        f.write(f"master's public ip is {master.public_ip}\n")
        f.write(f"Yarn地址 http://{master.public_ip}:8034\n")
        f.write(f"Spark History地址 http://{master.public_ip}:18080\n")
        # f.write(f"火焰图查看地址 http://{master.public_ip}:8089\n")
        f.close()

    with open(infofile, 'a+') as f:
        for line in f:
            print(line)


def conf_env(job, env_str):
    env_file = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/config/system/env.sh"

    with open(env_file, 'a+') as f:
        f.write(f"{env_str}")
        f.close()
        # upload and source env
    job.upload("target/" + CLUSTER_NAME + "/config/system/env.sh", "/etc/profile.d/env.sh")
    job.run('source /etc/profile.d/env.sh')


def init_disk(job):
    config = ConfigParser()

    config.read(CONF_PATH, encoding='UTF-8')

    disk_num = config['cmd']['total_disk_num']

    job.run('mkdir -p /root/' + CLUSTER_NAME + '/system')
    job.upload(FASTMR_PATH + '/trans/config/system', "/root/" + CLUSTER_NAME + "/system")

    # 如果是 windows 上传的 shell 需要转换下格式
    if os.name == 'nt':
        job.run('dos2unix /root/' + CLUSTER_NAME + '/* ')

    if config.has_option('cmd', 'local_disk_type') and config.get('cmd', 'local_disk_type') == 'nvme':
        job.run('sh /root/' + CLUSTER_NAME + '/system/mkfs_nvme.sh ' + disk_num)
    else:
        job.run('sh /root/' + CLUSTER_NAME + '/system/mkfs-ad.sh ' + disk_num)


def conf_hadoop(job):
    config = ConfigParser()

    config.read(CONF_PATH, encoding='UTF-8')

    hadoop_version = config['hadoop']['version']
    instancename = config['ncluster']['instancename']
    disk_num = config.getint('cmd', 'total_disk_num')
    # # system hosts file
    hostsstr = ""
    slavesstr = ""
    for i in range(len(job.tasks)):
        task = job.tasks[i]
        instance = task.instance
        ip = instance.private_ip()
        hostname = instance.host_name()
        slavesstr += f"{hostname} \n"
        if i == 0:
            hostsstr += f"{ip}  {hostname}  master1 \n"
        else:
            hostsstr += f"{ip}  {hostname} \n"

    hostfile = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/config/system/hosts"
    with open(hostfile, 'a+') as f:
        f.write(f"{hostsstr}")
        f.close()

    # clushtershell cfgfile
    # all: task[0-2].mracc-d2s
    clushfile = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/config/system/local.cfg"
    with open(clushfile, 'a+') as f:
        f.write(f"all: {instancename}[0-{len(job.tasks) - 1}]")
        f.close()

    job.upload(FASTMR_PATH + "/target/" + CLUSTER_NAME + "/config/system/hosts", "/etc/hosts")
    job.run("mkdir -p /etc/clustershell/groups.d")
    job.upload(FASTMR_PATH + "/target/" + CLUSTER_NAME + "/config/system/local.cfg",
               "/etc/clustershell/groups.d/local.cfg")

    # hdfs-site.xml
    # slaves
    slavesfile = FASTMR_PATH + "/target/" + CLUSTER_NAME + f"/config/hadoop-{hadoop_version}/slaves"
    if hadoop_version == '3.2.1' or hadoop_version == '3.3.1':
        slavesfile = FASTMR_PATH + "/target/" + CLUSTER_NAME + f"/config/hadoop-{hadoop_version}/workers"
    with open(slavesfile, 'a+') as f:
        f.write(f"{slavesstr}")
        f.close()

    hdfsdatadir = ""
    yarnlocaldir = ""

    for i in range(disk_num - 1):
        if i == 0:
            hdfsdatadir += f"/mnt/disk{i + 1}/data/hadoop"
            yarnlocaldir += f"/mnt/disk{i + 1}/data/nmlocaldir"
        else:
            hdfsdatadir += f",/mnt/disk{i + 1}/data/hadoop"
            yarnlocaldir += f",/mnt/disk{i + 1}/data/nmlocaldir"

    hdfssitefile = FASTMR_PATH + "/target/" + CLUSTER_NAME + f"/config/hadoop-{hadoop_version}/hdfs-site.xml"
    hdfstree = ET.parse(hdfssitefile)

    root = hdfstree.getroot()
    for iproperty in root.findall("property"):
        # print(iproperty.find("name").text,iproperty.find("value").text)
        if iproperty.find("name").text == "dfs.datanode.data.dir":
            iproperty.find("value").text = hdfsdatadir
    hdfstree.write(hdfssitefile)

    # yarn-site.xml

    vcpunum = job.tasks[0].instance.cpu()
    meminfo = job.tasks[0].instance.memory()
    yarnsitefile = FASTMR_PATH + "/target/" + CLUSTER_NAME + f"/config/hadoop-{hadoop_version}/yarn-site.xml"
    yarntree = ET.parse(yarnsitefile)
    root = yarntree.getroot()
    for iproperty in root.findall("property"):
        if iproperty.find("name").text == "yarn.nodemanager.local-dirs":
            iproperty.find("value").text = yarnlocaldir
        if iproperty.find("name").text == "yarn.nodemanager.resource.memory-mb":
            iproperty.find("value").text = str(meminfo)
        if iproperty.find("name").text == "yarn.scheduler.maximum-allocation-mb":
            iproperty.find("value").text = str(meminfo)
        if iproperty.find("name").text == "yarn.nodemanager.resource.cpu-vcores":
            iproperty.find("value").text = str(vcpunum)
        if iproperty.find("name").text == "yarn.scheduler.maximum-allocation-vcores":
            iproperty.find("value").text = str(vcpunum)
    yarntree.write(yarnsitefile)

    job.upload(f"{FASTMR_PATH}/target/{CLUSTER_NAME}/config/hadoop-{hadoop_version}",
               f"/opt/hadoop-{hadoop_version}/etc/hadoop")
    # 如果是 windows 上传的 shell 需要转换下格式
    if os.name == "nt":
        job.run(f"dos2unix /opt/hadoop-{hadoop_version}/etc/hadoop/*")


def conf_spark(job):
    config = ConfigParser()
    config.read(CONF_PATH, encoding='UTF-8')

    spark_version = config['spark']['version']

    spark_conf = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/config/spark/spark-defaults.conf"
    job.upload(spark_conf, f"/opt/spark-{spark_version}/conf")


def start_cluster(master, job):
    config = ConfigParser()

    config.read(CONF_PATH, encoding='UTF-8')

    hadoop_version = config['hadoop']['version']
    spark_version = config['spark']['version']
    disk_num = config['cmd']['total_disk_num']

    master.run(f"/opt/hadoop-{hadoop_version}/sbin/stop-yarn.sh")
    master.run(f"/opt/hadoop-{hadoop_version}/sbin/stop-dfs.sh")
    # 所有节点 重新初始化集群之前先清理hdfs目录
    job.run("for i in {1.." + disk_num + "};do rm -rf /mnt/disk$i/data/hadoop; done")
    master.run("hdfs namenode -format -force")
    master.run(f"/opt/hadoop-{hadoop_version}/sbin/start-dfs.sh")
    master.run("hadoop fs -mkdir /sparklogs")
    master.run(f"/opt/hadoop-{hadoop_version}/sbin/start-yarn.sh")
    # 重启 spark history
    master.run(f"/opt/spark-{spark_version}/sbin/stop-history-server.sh")
    master.run(f"/opt/spark-{spark_version}/sbin/start-history-server.sh")

    print(f"browser yarn from http://{master.public_ip}:8034")

    if not master.exists("/usr/bin/mysql"):

        master.run('yum install -y mysql-server.x86_64')
        # install driver
        mysql_connect_jar(master)
        # 启动 meta store db mysql
        config_mysql(master)

        # 解决guava.jar版本问题
        align_guava(master)

        # hive初始化
        master.run('schematool -dbType mysql -initSchema')
    else:
        master.run("service mysqld restart")


def start_flame(master):
    # 火焰图
    master.run("hadoop fs -mkdir -p /tmp/profiler/")
    master.run("mkdir -p /root/flame/")
    master.upload(FASTMR_PATH + "/target/" + CLUSTER_NAME + "/config/flame/async-profiler-1.8.3-linux-x64.tar.gz",
                  "/root/flame/")
    master.run("hadoop fs -put -f /root/flame/async-profiler-1.8.3-linux-x64.tar.gz /tmp/profiler/")

# 在fastmr上调参的关键
def compute_spark_conf(worker):
    config = ConfigParser()

    config.read(CONF_PATH, encoding='UTF-8')

    vcpunum = worker.instance.cpu()
    meminfo = worker.instance.memory()

    machines = config.getint('ncluster', 'machines')

    if config.has_option('spark', 'executor_core'):
        executor_core = config.getint('spark', 'executor_core')
    else:
        executor_core = 4
    SPARK_EXECUTOR_INSTANCES = (math.floor(vcpunum / executor_core) - 1) * machines

    if config.has_option('spark', 'executor_mem'):
        SPARK_EXECUTOR_MEMORY = config.getint('spark', 'executor_mem')
    else:
        SPARK_EXECUTOR_MEMORY = math.floor(meminfo / 1024 / (SPARK_EXECUTOR_INSTANCES / machines) * 0.9)

    NUM_REDUCERS = SPARK_EXECUTOR_INSTANCES * 4
    SPARK_EXECUTOR_MEMORYOVERHEAD = math.floor(SPARK_EXECUTOR_MEMORY * 0.5)
    if SPARK_EXECUTOR_MEMORYOVERHEAD < 1:
        SPARK_EXECUTOR_MEMORYOVERHEAD = 1
    SPARK_DEFAULT_PARALLELISM = NUM_REDUCERS * 3

    return SPARK_EXECUTOR_INSTANCES, SPARK_EXECUTOR_MEMORY, NUM_REDUCERS, SPARK_EXECUTOR_MEMORYOVERHEAD, \
           SPARK_DEFAULT_PARALLELISM, executor_core


def run_tpcxhs(master, tpcxhs_scaleFactor, SPARK_EXECUTOR_INSTANCES, SPARK_EXECUTOR_MEMORY,
               SPARK_DEFAULT_PARALLELISM,
               SPARK_EXECUTOR_CORES):
    usedconf = "spark-config.conf.flame"

    for line in fileinput.input(f"{FASTMR_PATH}/target/{CLUSTER_NAME}/tpcxhs/{usedconf}", inplace=True):

        if line.__contains__('spark.driver.cores'):
            print(f"spark.driver.cores {SPARK_EXECUTOR_CORES}")
            continue
        if line.__contains__('spark.driver.memory'):
            print(f"spark.driver.memory {SPARK_EXECUTOR_MEMORY}g")
            continue
        if line.__contains__('spark.executor.instances'):
            print(f"spark.executor.instances {SPARK_EXECUTOR_INSTANCES}")
            continue
        if line.__contains__('spark.executor.cores'):
            print(f"spark.executor.cores {SPARK_EXECUTOR_CORES}")
            continue
        if line.__contains__('spark.executor.memory '):
            print(f"spark.executor.memory {SPARK_EXECUTOR_MEMORY}g")
            continue
        if line.__contains__('spark.default.parallelism'):
            print(f"spark.default.parallelism {SPARK_DEFAULT_PARALLELISM}")
            continue
        else:
            print(line, end='')

    for line in fileinput.input(f"{FASTMR_PATH}/target/{CLUSTER_NAME}/tpcxhs/Benchmark_Parameters.sh", inplace=True):
        if line.__contains__('SPARK_CONF'):
            print(f'SPARK_CONF="{usedconf}"')
            continue
        else:
            print(line, end='')

    # set the size of tpcxhs
    tpcxhs_command = f"cd /opt/TPC/TPCx-HS/ \n" \
                     f"chmod 755 TPCx-HS-master.sh.withconf \n" \
                     f"./TPCx-HS-master.sh.withconf -s -g {tpcxhs_scaleFactor}"
    tpcxhs_script = f"{FASTMR_PATH}/target/{CLUSTER_NAME}/tpcxhs/runtpcxhs.sh"

    with open(tpcxhs_script, encoding="utf-8", mode="a") as file:
        file.write(tpcxhs_command)
    master.upload(f"{FASTMR_PATH}/target/{CLUSTER_NAME}/tpcxhs", "/opt/TPC/TPCx-HS/")

    if os.name == "nt":
        master.run("dos2unix /opt/TPC/TPCx-HS/*")

    print("TPCx-HS is running")
    tpcxhs_start_time = time.time()
    master.run("sh /opt/TPC/TPCx-HS/runtpcxhs.sh")
    eclapse_time = time.time() - tpcxhs_start_time
    # print(f'tpcxhs deploy time is: {eclapse_time} s.')

    infofile = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/cluster.info"
    with open(infofile, 'a+') as f:
        f.write("-------------TPCx-HS---------------\n")
        f.write(f"TPCx-HS run time : {eclapse_time} \n")

# 运行tpcds的程序
def run_tpcds(master, tpcds_scaleFactor, SPARK_EXECUTOR_INSTANCES, SPARK_EXECUTOR_MEMORY,
              SPARK_EXECUTOR_MEMORYOVERHEAD,
              SPARK_DEFAULT_PARALLELISM,
              SPARK_EXECUTOR_CORES):
    # configure tpcds spark.config
    # 这里是在生成spark-config.conf
    # 我的conf文件最终应该就是要替换这个文件
    # {FASTMR_PATH}/target/{CLUSTER_NAME}/tpcds/spark-config.conf
    for line in fileinput.input(f"{FASTMR_PATH}/target/{CLUSTER_NAME}/tpcds/spark-config.conf", inplace=True):
        if line.__contains__('spark.driver.cores'):
            print(f"spark.driver.cores {2}")
            continue
        if line.__contains__('spark.driver.memory'):
            print(f"spark.driver.memory {4}g")
            continue
        if line.__contains__('spark.executor.instances'):
            print(f"spark.executor.instances {5}")
            continue
        if line.__contains__('spark.executor.cores'):
            print(f"spark.executor.cores {2}")
            continue
        if line.__contains__('spark.executor.memory '):
            print(f"spark.executor.memory {3}g")
            continue
        if line.__contains__('spark.executor.memoryOverhead'):
            print(f"spark.executor.memoryOverhead {1}g")
            continue
        if line.__contains__('spark.default.parallelism'):
            print(f"spark.default.parallelism {50}")
            continue
        else:
            print(line, end='')

    # set the size of tpcds
    tpcds_datagen_command = f"spark-submit --properties-file spark-config.conf --class " \
                            f"com.databricks.spark.sql.perf.tpcds.TPCDS_Bench_DataGen " \
                            f"spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar hdfs://master1:9000/tmp/tpcds_{tpcds_scaleFactor} " \
                            f"tpcds_{tpcds_scaleFactor} {tpcds_scaleFactor} parquet"
    tpcds_runallsql_command = f"spark-submit --properties-file spark-config.conf --class " \
                              f"com.databricks.spark.sql.perf.tpcds.TPCDS_Bench_RunAllQuery " \
                              f"spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar all hdfs://master1:9000/tmp/tpcds_{tpcds_scaleFactor}" \
                              f" tpcds_{tpcds_scaleFactor} /tmp/tpcds_{tpcds_scaleFactor}_result "

    tpcds_datagen_script = f"{FASTMR_PATH}/target/{CLUSTER_NAME}/tpcds/datagen_custom.sh"
    tpcds_runallsql_script = f"{FASTMR_PATH}/target/{CLUSTER_NAME}/tpcds/runallquery_custom.sh"
    with open(tpcds_datagen_script, encoding="utf-8", mode="a") as file:
        file.write("cd /opt/TPC/TPC-DS \n")
        file.write(tpcds_datagen_command)
    with open(tpcds_runallsql_script, encoding="utf-8", mode="a") as file:
        file.write("cd /opt/TPC/TPC-DS \n")
        file.write(tpcds_runallsql_command)
    master.upload(f"{FASTMR_PATH}/target/{CLUSTER_NAME}/tpcds", "/opt/TPC/TPC-DS/")
    print("TPC-DS is running")
    tpcds_gen_start_time = time.time()
    master.run("/opt/TPC/TPC-DS/datagen_custom.sh")
    tpcds_gen_time = time.time() - tpcds_gen_start_time
    tpcds_sql_start_time = time.time()
    master.run("/opt/TPC/TPC-DS/runallquery_custom.sh")

    tpcds_sql_time = time.time() - tpcds_sql_start_time

    # eclapse_time = time.time() - tpcds_start_time
    # print(f'tpcds deploy time is: {eclapse_time} s.')

    infofile = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/cluster.info"
    with open(infofile, 'a+') as f:
        f.write("-------------TPC-DS---------------\n")
        f.write(f"TPC-DS-Gen run time : {tpcds_gen_time} s\n")
        f.write(f"TPC-DS-SQL run time : {tpcds_sql_time} s\n")


def show_result():
    infofile = FASTMR_PATH + "/target/" + CLUSTER_NAME + "/cluster.info"
    for line in open(infofile):
        print(line)


# 自己重写这个方法
def run_tpc(job):
    config = ConfigParser()

    config.read(CONF_PATH, encoding='UTF-8')

    SPARK_EXECUTOR_INSTANCES, SPARK_EXECUTOR_MEMORY, NUM_REDUCERS, SPARK_EXECUTOR_MEMORYOVERHEAD, \
    SPARK_DEFAULT_PARALLELISM, SPARK_EXECUTOR_CORES = compute_spark_conf(job.tasks[0])

    # 运行测试
    runTPCDS = config.getboolean('tpcds', 'run')
    if runTPCDS:
        tpcds_scaleFactor = config['tpcds']['scaleFactor']
        run_tpcds(job.tasks[0],
                  tpcds_scaleFactor,
                  SPARK_EXECUTOR_INSTANCES=SPARK_EXECUTOR_INSTANCES,
                  SPARK_EXECUTOR_MEMORY=SPARK_EXECUTOR_MEMORY,
                  SPARK_EXECUTOR_MEMORYOVERHEAD=SPARK_EXECUTOR_MEMORYOVERHEAD,
                  SPARK_DEFAULT_PARALLELISM=SPARK_DEFAULT_PARALLELISM,
                  SPARK_EXECUTOR_CORES=SPARK_EXECUTOR_CORES)
    runTPCxHS = config.getboolean('tpcxhs', 'run')
    if runTPCxHS:
        tpcxhs_scaleFactor = config['tpcxhs']['scaleFactor']
        run_tpcxhs(job.tasks[0],
                   tpcxhs_scaleFactor,
                   SPARK_EXECUTOR_INSTANCES=SPARK_EXECUTOR_INSTANCES,
                   SPARK_EXECUTOR_MEMORY=SPARK_EXECUTOR_MEMORY,
                   SPARK_DEFAULT_PARALLELISM=SPARK_DEFAULT_PARALLELISM,
                   SPARK_EXECUTOR_CORES=SPARK_EXECUTOR_CORES)
    show_result()
    print(f"complete TPC test.You can view the results in ./target/{CLUSTER_NAME}/cluster.info")
