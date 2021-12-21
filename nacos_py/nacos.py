'''
@project: nacos-py
@name: nacos.py
@date: 2021/12/21 11:07
@IDE: PyCharm
@author：arckal sun
@email: arckalsun@gmail.com
'''
import multiprocessing
import configparser
import platform
import time
import requests
import os
from threading import Thread
from pathlib import Path
from nacos_py import NacosClient
from nacos_py.listener import SubscribeListener


# Both HTTP/HTTPS protocols are supported, if not set protocol prefix default is HTTP, and HTTPS with no ssl check(verify=False)
# "192.168.3.4:8848" or "https://192.168.3.4:443" or "http://192.168.3.4:8848,192.168.3.5:8848" or "https://192.168.3.4:443,https://192.168.3.5:443"



def singleton(cls):
    instances = {}

    def _singleton(*_args, **_kwargs):
        key = str(cls) + str(os.getpid())
        if key not in instances:
            instances[key] = cls(*_args, **_kwargs)
        return instances[key]

    return _singleton

class NacosConfig(object):
    '''
    Nacos 配置中心 配置类
    配置文件格式：ini
    每次启动，自动从远程加载配置
    '''
    root_dir = Path(__file__).resolve().parent

    def __init__(self, data_id, server_addresses, namespace=None, group='DEFAULT_GROUP', username=None, password=None):
        '''
        Nacos 配置中心
        '''
        self.data_id = data_id
        self.group = group
        self.client = NacosClient(server_addresses, namespace=namespace, username=username,
                                        password=password)
        self.conf = configparser.ConfigParser()

        if platform.system() == 'Windows':
            if multiprocessing.current_process().name == "MainProcess":
                self.client.add_config_watcher(self.data_id, self.group, self.on_change)
        else:
            self.client.add_config_watcher(self.data_id, self.group, self.on_change)

    def get(self, key, section='default'):
        try:
            content = self.client.get_config(self.data_id, self.group)
            self.conf.read_string(content)
            return self.conf.get(section, key)
        except configparser.NoOptionError:
            return None

    def on_change(self, data):
        '''
        处理相应配置更新后，刷新配置
        注意数据库，redis配置，如有必要刷新已建立的连接
        :param data:
        :return:
        '''
        self.conf.read_string(data['content'])

    def __getattribute__(self, item):
        if item.isupper():
            return self.get(item) or super().__getattribute__(item)
        else:
            return super().__getattribute__(item)

@singleton
class NacosService(object):
    '''
    Nacos 注册中心
    '''
    def __init__(self, service_name, server_addresses, namespace=None, username=None, password=None):
        self.service_name = service_name
        self.client = NacosClient(server_addresses, namespace=namespace, username=username,
                    password=password)
        self.beating = True
        self.heartbeatThread = None
        self.headers = {
            "Content-Type": "application/json",
            "Connection": "close",
            "service": service_name
        }

    def subscribe(self, service_name):
        listener_fn = lambda x: print(x)
        listener = SubscribeListener(listener_fn, service_name)
        self.client.subscribe(listener, service_name=service_name)


    def register(self, ip, port, cluster_name=None, **kwargs):
        '''
        注册服务
        :param ip:
        :param port:
        :param cluster_name:
        :return:
        '''
        self.client.add_naming_instance(self.service_name, ip, port, cluster_name=None, **kwargs)
        self.heartbeatThread = Thread(target=self.run_heartbeat, args=(self.service_name, ip, port, cluster_name,
                                                                       kwargs.get("weight"), kwargs.get("metadata"),), daemon=True)
        self.heartbeatThread.setName("nacos-heartbeat")
        self.heartbeatThread.start()

    def query(self, service_name):
        '''
        查询服务
        :param service_name:
        :param clusters:
        :param namespace_id:
        :param group_name:
        :param healthy_only:
        :return:
        '''
        # 从本地缓存读取
        instances = self.client.subscribed_local_manager.get_local_instances(service_name)
        host = {}
        for k, v in instances.items():
            host = v.instance
            if host['healthy']:
                break
        # instances = self.client.list_naming_instance(service_name, **kwargs)
        # 这里可以做负载均衡策略，默认取第一个host
        instance = f'http://{host["ip"]}:{host["port"]}'
        return instance

    def run_heartbeat(self, service_name, ip, port, cluster_name, weight, metadata):
        '''
        开启心跳
        :return:
        '''
        while self.beating:
            interval = 5
            try:
                data = self.client.send_heartbeat(service_name, ip, port, cluster_name, weight, metadata)
                interval = int(data['clientBeatInterval'])/1000
            except:
                pass
            finally:
                time.sleep(interval)

    def request(self, method, service, endpoint, params=None, data=None, try_times=3):
        '''
        调远程服务
        :param method:  GET, POST, etc
        :param service: Nacos service name
        :param endpoint: Nacos endpoint name
        :param param:    request param
        :param headers:  request headers
        :return:
        '''
        exception = None
        for t in range(try_times):
            try:
                headers = self.headers.copy()
                ins = self.query(service)
                url = ins + endpoint
                action = getattr(requests, method.lower(), None)
                response = action(url=url, data=data, params=params, headers=headers)
                return response
            except Exception as e:
                exception = e
                time.sleep(2 * (t+1))
        else:
            raise Exception(f'remote service [{service}] http request error: {exception}')