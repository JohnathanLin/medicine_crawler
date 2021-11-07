#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/12/23 14:05
# @Author  : Johnathan Lin 凌虚
"""
发送请求模块
"""

import json
import socket
import time
import random
import chardet
import requests
import utils.globalVariableManager as globalVariable
from utils.logger import all_console_log
socket.setdefaulttimeout(20)


def create_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36'
    }


def request(url, request_type, headers, data):
    """
    发送请求
    :param url: 请求路径
    :param request_type:  请求类型（get或post）
    :param headers: 请求头
    :param data: 请求参数对象
    :param open_proxy:  是否使用代理
    :return:  请求得到的文本
    """
    # 代理IP类
    proxies = {}
    # 关闭警告
    requests.packages.urllib3.disable_warnings()
    response = ''
    # 如果使用代理ip
    # open_proxy = globalVariable.get_value('open_proxy')
    # 请求间隔休息时间
    sleep_time = globalVariable.get_value('sleep_time')
    try:
        # if open_proxy == 'True' or open_proxy == 'true':
        #     now = int(time.time())
        #     # 若从未读取过代理IP或距离上次读取10分钟以上，则重新读取
        #     if len(proxies.keys()) == 0 or (now - globalVariable.get_value('proxy_timestamp') > 600):
        #         response = requests.get('http://xxxx:10028/bigdata_proxy/ipServer/getBestIp/3')
        #         html = str(response.content, encoding="utf-8")
        #         ip = json.loads(html)
        #         proxies['http'] = 'http://xxx:wt024zjg@' + ip['datas'][0]['ip'] + ':' + str(ip['datas'][0]['port']) + '/'
        #         # print('请求' + url + '时使用代理，代理的地址为:' + proxies['http'])
        #         globalVariable.set_value('proxy_timestamp', now)
        if request_type == 'get':
            response = requests.get(url=url, headers=headers, data=data, proxies=proxies, timeout=60, verify=False)
        elif request_type == 'post':
            response = requests.post(url=url, headers=headers, data=data, proxies=proxies, timeout=60, verify=False)
        # print(response.content)
        result = response.content
        response.close()
    except BaseException as e:
        if 'Remote end closed connection without response' in str(e):
            print(url, json.dumps(data))
            return None
        else:
            raise e
    # 请求间隔

    if type(sleep_time) is list and  len(sleep_time) == 2 and sleep_time[0] < sleep_time[1]:
        sleep_time = random.randint(sleep_time[0], sleep_time[1])
        time.sleep(sleep_time)
    elif type(sleep_time) is int and sleep_time > 0:
        time.sleep(sleep_time)
    return str(result, encoding=chardet.detect(result)['encoding'], errors='ignore')


def request_while_response(url, type, headers, data, sleep_time):
    """
   发送请求,规避10054错误，直至请求成功
   :param url: 请求路径
   :param type:  请求类型（get或post）
   :param headers: 请求头
   :param data: 请求参数对象
   :param open_proxy:  是否使用代理
   :return:  请求得到的文本
   """
    retry_count = globalVariable.get_value('retry_count', 20)
    r = None
    num = 0
    for i in range(retry_count):  # 循环
        try:
            r = request(url, type, headers, data)
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            if 'bad handshake' in str(e) or '10054' in str(e) or 'Read timed out' in str(e) or 'Name or service not known' in str(e):  # 上述2种异常
                print('请求url：' + url + '时发生错误:' + str(e) +',\n过' + str(sleep_time) + '秒重新请求')
                time.sleep(sleep_time)
                num = num + 1
                continue  # 继续发请求
            else:
                raise Exception(e)  # 其他异常，抛出来
        break  # 无异常就跳出循环
    if num == retry_count:
        print('重试请求url:' + url + ' '+str(retry_count)+ '次，未能爬取成功')
    return r  # 返回响应结果
