#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/12/23 14:02
# @Author  : Johnathan Lin 凌虚
"""
数据导入Mongo模块
"""

import os

import pymongo

from utils.configReader import read_mongo_config
import utils.globalVariableManager as globalVariable
# 设置编码
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def get_mongodb_collection(collection):
    """
    根据配置文件和数据库和表得到表（collection）
    :param database: 数据库
    :param collection: 表
    :return:  表的对象
    """
    mongo_config = globalVariable.get_value('mongo_config')
    client = pymongo.MongoClient(mongo_config['client'])
    db = client[mongo_config['database']]
    if mongo_config['auth'] == 'True' or mongo_config['auth'] == 'true':
        db.authenticate(mongo_config['username'], mongo_config['password'])
    col = db[collection]
    return col


def insert_data(collection, document):
    """
    将数据插入MongoDB中
    :param collection:数据将插入的集合名
    :param doc:数据文档
    :return:无
    """
    col = get_mongodb_collection(collection)
    col.insert_one(document)


def insert_data_list(collection, document_list):
    """
    批量插入数据到MongoDB
    :param collection: 数据将插入的集合名
    :param document_list: 数据文档列表
    :return:
    """
    col = get_mongodb_collection(collection)
    col.insert_many(document_list)


def get_children_classify_id(parent_id):
    """
    获取所有子孙节点
    :param parent_id: 父节点ID
    :return: 所有子孙节点
    """
    col = get_mongodb_collection('crawler_classify')
    res = col.aggregate([
        {'$match': {'classify_id': parent_id}},
        {'$graphLookup': {'from': 'crawler_classify', 'startWith': '$classify_id', 'connectFromField': 'classify_id',
                          'connectToField': 'parent_id', 'as': 'son'}}
    ])
    res_list = []
    for doc in res:
        res_list.append(doc)
    if len(res_list) == 0 or len(res_list[0]['son']) == 0:
        return []
    else:
        return [classify['classify_id'] for classify in res_list[0]['son']]


def get_document_by_condition(collection, key, value):
    """
    根据集合和单一条件查找匹配的文档
    :param collection:  集合名
    :param key:  键
    :param value: 值
    :return: 匹配的文档列表
    """
    col = get_mongodb_collection(collection)
    res = col.find({key: value})
    res_list = []
    for doc in res:
        res_list.append(doc)
    return res_list


def update_document(collection, condition_key, condition_value, updated_key, new_value):
    """
    更新文档
    :param collection:
    :param condition_key:
    :param condition_value:
    :param updated_key:
    :param new_value:
    :return:
    """
    col = get_mongodb_collection(collection)
    col.update({condition_key: condition_value}, {'$set': {updated_key: new_value}})


def get_document_by_condition_list(collection, condition_list):
    """
    多条件同时成立查询匹配的文档
    :param collection: 集合名
    :param condition_list:  条件列表（列表中的元素为{key:value}形式）
    :return: 匹配的文档
    """
    col = get_mongodb_collection(collection)
    res = col.find({'$and': condition_list})
    res_list = []
    for doc in res:
        res_list.append(doc)
    return res_list


def remove_document(collection, condition_list):
    """
    删除文档
    :param collection: 删除文档的集合
    :param condition_list:
    :return:
    """
    col = get_mongodb_collection(collection)
    col.remove({'$and': condition_list})


def update_document_condition_list_and_new_value_obj(collection, condition_list, new_value_obj):
    """
    更新update文档，通过多个用“&”连接的条件和新对象更新
    :param collection: 集合
    :param condition_list: 与条件的列表
    :param new_value_obj: 要更新的新值对象
    :return:
    """
    col = get_mongodb_collection(collection)
    col.update({'$and': condition_list}, {'$set': new_value_obj})
