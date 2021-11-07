#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/12/23 14:05
# @Author  : Johnathan Lin 凌虚
"""
配置读取模块
"""
import configparser


def read_crawler_config():
    """
    读取爬虫任务配置文件
    :return: 存有爬虫任务配置的对象
    """
    config = configparser.ConfigParser()
    config.read('config/crawler.ini')
    return {
        'thread_number': int(config['THREADNUMBER']['threadNumber']),
        'sleep_time': config['SLEEPTIME']['sleepTime'],
        'retry_count': int(config['RETRYCOUNT']['retryCount']),
    }


def read_mongo_config():
    """
    读取MongoDB配置文件
    :return: 存有MongoDB配置的对象
    """
    config = configparser.ConfigParser()
    config.read('config/mongodb.ini')
    return {
        'client': config['CONFIG']['client'],
        'auth': config['CONFIG']['auth'],
        'username': config['CONFIG']['username'],
        'password': config['CONFIG']['password'],
        'database': config['CONFIG']['database']
    }
