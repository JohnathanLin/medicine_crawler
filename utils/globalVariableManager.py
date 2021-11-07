#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/2/7 11:46
# @Author  : Johnathan Lin 凌虚
"""
全局变量
"""
import time

def _init():
    """
    全局变量类初始化
    :return:
    """
    global _global_dict
    _global_dict = {}

    # 初始化代理IP时间戳
    # _global_dict['proxy_timestamp'] = int(time.time())


def set_value(name, value):
    """
    设置全局变量
    :param name: 键
    :param value: 值
    :return:
    """
    _global_dict[name] = value


def get_value(name, defValue=None):
    """
    读取全局变量
    :param name: 键
    :param defValue: 默认返回值
    :return: 值
    """
    try:
        return _global_dict[name]
    except KeyError:
        return defValue