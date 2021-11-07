#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/12/23 14:04
# @Author  : Johnathan Lin 凌虚
"""
日志输出
"""
import logging
import time
from logging import handlers

from utils.dataOperator import insert_data

# 结果日志MongoDB集合
mongodb_result_log_name = 'crawler_result_log'
# 错误日志MongoDB集合
mongodb_error_log_name = 'crawler_error_log'
# 结果字典
result_dict = {
    'success': '1',
    'failure': '0'
}


def error_log(section, classify_id, batch_number, url, error_message):
    """
    输出错误日志到Mongo中
    :param section: 环节
    :param classify_id: 分类ID
    :param batch_number: 批次号
    :param url: url
    :param error_message: 报错信息
    :return:
    """
    global mongodb_error_log_name
    obj = {
        'section': section,
        'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
        'classify_id': classify_id,
        'batch_number': batch_number,
        'url': url,
        'error_message': error_message
    }
    insert_data(mongodb_error_log_name, obj)


def result_log(section, cost_time, classify_id, batch_number, result, success_num='', failure_num=''):
    """
    输出结果日志到Mongo中
    :param section: 环节（url或document）
    :param cost_time： 消耗的时间(s)
    :param classify_id: 分类ID
    :param batch_number: 批次号
    :param result: 结果(success或failure）
    :param success_num: 成功数
    :param failure_num: 失败数
    :return:
    """
    global mongodb_result_log
    obj = {}
    if section == 'url':
        obj = {
            'section': section,
            'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            'cost_time': cost_time,
            'classify_id': classify_id,
            'batch_number': batch_number,
            'result': result_dict[result],
            'success_num': '',
            'failure_num': ''
        }
    elif section == 'document':
        obj = {
            'section': section,
            'time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            'cost_time': cost_time,
            'classify_id': classify_id,
            'batch_number': batch_number,
            'result': result_dict[result],
            'success_num': str(success_num),
            'failure_num': str(failure_num)
        }
    insert_data(mongodb_result_log_name, obj)


class Logger(object):
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }  # 日志级别关系映射

    def __init__(self, filename, level='info', when='D', backCount=3,
                 fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)  # 设置日志格式
        self.logger.setLevel(self.level_relations.get(level))  # 设置日志级别
        sh = logging.StreamHandler()  # 往屏幕上输出
        sh.setFormatter(format_str)  # 设置屏幕上显示的格式
        th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=backCount,
                                               encoding='utf-8')  # 往文件里写入#指定间隔时间自动生成文件的处理器
        # 实例化TimedRotatingFileHandler
        # interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
        # S 秒
        # M 分
        # H 小时、
        # D 天、
        # W 每星期（interval==0时代表星期一）
        # midnight 每天凌晨
        th.setFormatter(format_str)  # 设置文件里写入的格式
        self.logger.addHandler(sh)  # 把对象加到logger里
        self.logger.addHandler(th)


# 全局的日志输出
all_console_log = Logger('all.log', level='debug')
# 错误的日志输出
err_console_log = Logger('err.log', level='error')
