#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/8/7 10:43
# @Author  : Johnathan Lin 凌虚
from bson import ObjectId

from utils.configReader import read_crawler_config, read_mongo_config

from utils.logger import error_log, all_console_log, err_console_log
import utils.globalVariableManager as globalVariable
from utils.dataOperator import insert_data, get_mongodb_collection, insert_data_list
from utils.requester import request, create_headers
import sys
import queue
import threading
import traceback

import json

lock = threading.Lock()

def crawler(url):
    html = request(url, 'get', create_headers(), None)
    res = {'html': html}
    return res

def crawl_data(urlQueue, mongo_collection):
    """
    执行爬虫
    :param classify_id: 分类ID
    :param batch_number: 批次号
    :param urlQueue: Url队列
    :param source_web: 数据来源字符串
    :param province_classify: 省份分类对象
    :param city_classify: 城市分类对象
    :return:
    """
    while True:
        if urlQueue.empty():
            break
        try:
            # 不阻塞的读取队列数据
            lock.acquire()
            url = urlQueue.get_nowait()
            lock.release()
            i = urlQueue.qsize()
        except Exception as e:
            break

        try:
            all_console_log.logger.info('正在爬取：' + url +
                '，还剩' + str(i)+ '条未爬取')
            # 根据不同的分类爬取文档
            result_document = crawler(url)

            # 将得到的文档插入Mongo数据库中
            insert_data(mongo_collection, result_document)
            # 更新Url表中该Url的状态为 已爬取

        except BaseException as e:
            traceback.print_exc()
            err_obj = {'err_url': url, 'exception': repr(e)}
            insert_data("1_error", err_obj)
            all_console_log.logger.info(
                '错误信息：' + traceback.format_exc())
            err_console_log.logger.info(
                '错误信息：' + traceback.format_exc())



def get_document(page_num, thread_number, collection_name):
    """
    爬取文档
    :param classify_id: 分类ID
    :param batch_number: 批次号
    :param thread_number: 线程数
    :return:
    """
    # 获取省份和城市信息
    parent_url = 'https://api.dayi.org.cn:9997/api/medical/getList?pageSize=10&pageNo='
    # 将所有Url装进队列，用于多线程爬取
    urlQueue = queue.Queue()
    for page in range(page_num):
        # url_queue.put(url_obj['url'])
        url = parent_url + str(page + 1)
        urlQueue.put(url)
    threads = []

    # 可以调节线程数， 进而控制抓取速度
    for i in range(0, thread_number):
        t = threading.Thread(target=crawl_data,
                             args=( urlQueue, collection_name))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        # 多线程多join的情况下，依次执行各线程的join方法, 这样可以确保主线程最后退出， 且各个线程间没有阻塞
        t.join()

#===============================#

def geturl():
    # 初始化全局变量
    globalVariable._init()
    # 读取爬取相关的配置文件
    crawler_config = read_crawler_config()

    thread_number = crawler_config['thread_number']

    # 每次请求的间隔休息时间
    sleep_time = crawler_config['sleep_time']
    retry_count = crawler_config['retry_count']
    if sleep_time.find(',') != -1:
        sleep_time = [int(sleep_time.split(',')[0]), int(sleep_time.split(',')[1])]
    else:
        sleep_time = int(sleep_time)
    globalVariable.set_value('sleep_time', sleep_time)
    globalVariable.set_value('retry_count', retry_count)
    # 读取MongoDB相关的配置文件
    mongo_config = read_mongo_config()
    globalVariable.set_value('mongo_config', mongo_config)

    all_console_log.logger.info(
        '已读取配置文件信息。')

    # url_mongo_collection = "1_url"
    # mongo_url_detail_collection = '1_url_detail'
    # col = get_mongodb_collection(url_mongo_collection)
    # url_raw_doc = col.find()
    #
    # res_list = []
    # for url_raw in url_raw_doc:
    #     response_obj = json.loads(url_raw['html'])
    #     list = response_obj['list']
    #     for medicine in list:
    #
    #         detail_url = 'https://api.dayi.org.cn:9997/api/medical/' +str(medicine['id'])
    #
    #         detail_mongo_obj = {'url': detail_url}
    #         res_list.append(detail_mongo_obj)

#== 读取失败的记录 ==#
    url_mongo_collection = "1_error"
    mongo_url_detail_collection = '1_url_detail'
    col = get_mongodb_collection(url_mongo_collection)
    url_raw_doc = col.find()

    res_list = []
    for url_raw in url_raw_doc:
        detail_mongo_obj = {'url': url_raw['err_url']}
        res_list.append(detail_mongo_obj)

    res_obj = {'url_list': res_list}
    insert_data(mongo_url_detail_collection, res_obj)

#===============================#

def crawler_detail(url):
    html = request(url, 'get', create_headers(), None)
    obj = {'raw_json': html}
    return obj

def crawl_data_detail(urlQueue, mongo_collection):
    """
    执行爬虫
    :param classify_id: 分类ID
    :param batch_number: 批次号
    :param urlQueue: Url队列
    :param source_web: 数据来源字符串
    :param province_classify: 省份分类对象
    :param city_classify: 城市分类对象
    :return:
    """
    while True:
        if urlQueue.empty():
            break
        try:
            # 不阻塞的读取队列数据
            lock.acquire()
            url = urlQueue.get_nowait()
            lock.release()
            i = urlQueue.qsize()
        except Exception as e:
            break

        try:
            all_console_log.logger.info('正在爬取：' + url +
                '，还剩' + str(i)+ '条未爬取')
            # 根据不同的分类爬取文档
            result_document = crawler_detail(url)

            # 将得到的文档插入Mongo数据库中
            insert_data(mongo_collection, result_document)
            # 更新Url表中该Url的状态为 已爬取

        except BaseException as e:
            traceback.print_exc()
            err_obj = {'err_url': url, 'exception': repr(e)}
            insert_data("1_error", err_obj)
            all_console_log.logger.info(
                '错误信息：' + traceback.format_exc())
            err_console_log.logger.info(
                '错误信息：' + traceback.format_exc())




def getdetail():
    # 初始化全局变量
    globalVariable._init()
    # 读取爬取相关的配置文件
    crawler_config = read_crawler_config()

    thread_number = crawler_config['thread_number']

    # 每次请求的间隔休息时间
    sleep_time = crawler_config['sleep_time']
    retry_count = crawler_config['retry_count']
    if sleep_time.find(',') != -1:
        sleep_time = [int(sleep_time.split(',')[0]), int(sleep_time.split(',')[1])]
    else:
        sleep_time = int(sleep_time)
    globalVariable.set_value('sleep_time', sleep_time)
    globalVariable.set_value('retry_count', retry_count)
    # 读取MongoDB相关的配置文件
    mongo_config = read_mongo_config()
    globalVariable.set_value('mongo_config', mongo_config)

    all_console_log.logger.info(
        '已读取配置文件信息。')

    mongo_url_detail_collection = '1_url_detail'
    # TODO 这边放一个mongo 的ID 我我忘了
    doc = get_mongodb_collection(mongo_url_detail_collection).find_one({'_id': ObjectId('5f2d10d2374fa2286079e15b')})
    url_list = doc['url_list']

    collection_name = "1_medicine_row_json"
    # print(len(url_list))
    # 将所有Url装进队列，用于多线程爬取
    urlQueue = queue.Queue()
    for page in url_list:
        # url_queue.put(url_obj['url'])
        url = page
        urlQueue.put(url['url'])
    threads = []

    # 可以调节线程数， 进而控制抓取速度
    for i in range(0, thread_number):
        t = threading.Thread(target=crawl_data_detail,
                             args=(urlQueue, collection_name))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        # 多线程多join的情况下，依次执行各线程的join方法, 这样可以确保主线程最后退出， 且各个线程间没有阻塞
        t.join()

def schedule():
    # 初始化全局变量
    globalVariable._init()
    # 读取爬取相关的配置文件
    crawler_config = read_crawler_config()

    thread_number = crawler_config['thread_number']

    # 每次请求的间隔休息时间
    sleep_time = crawler_config['sleep_time']
    retry_count = crawler_config['retry_count']
    if sleep_time.find(',') != -1:
        sleep_time = [int(sleep_time.split(',')[0]), int(sleep_time.split(',')[1])]
    else:
        sleep_time = int(sleep_time)
    globalVariable.set_value('sleep_time', sleep_time)
    globalVariable.set_value('retry_count', retry_count)
    # 读取MongoDB相关的配置文件
    mongo_config = read_mongo_config()
    globalVariable.set_value('mongo_config', mongo_config)

    all_console_log.logger.info(
        '已读取配置文件信息。')

    url_mongo_collection = "1_url"

# TODO 这里写网站列表总页数
    page_num = 1815
    get_document(page_num, thread_number, url_mongo_collection)

def is_legal(data):
    return data != None and data != '' and data.strip() != ''

def parse_data_detail(urlQueue, row_collection, detail_collection):
    while True:
        if urlQueue.empty():
            break
        try:
            # 不阻塞的读取队列数据
            lock.acquire()
            url = urlQueue.get_nowait()
            lock.release()
            i = urlQueue.qsize()
        except Exception as e:
            break

        try:
            all_console_log.logger.info('正在爬取：' + str(url) +
                                        '，还剩' + str(i) + '条未爬取')
            col =  get_mongodb_collection(row_collection)
            raw_doc = col.find_one({'_id': url})

            raw_data = json.loads(raw_doc['raw_json'])
            res_data = {
                'id': '',
                '药品名称': '',
                '药品规格': '',
                '药品类型': '',
                '药品图片': '',
                '药品价格': '',
                '厂家': '',
                '主要功效': '',
                '适应病症': '',
                '用法用量': '',
                '不良反应': '',
                '禁忌': '',
                '注意事项': '',
            }
            if raw_data['medical'] != None:

                res_data['id'] = raw_data['medical']['id']

                if is_legal(raw_data['medical']['comName']):
                    res_data['药品名称'] = raw_data['medical']['comName']

                if is_legal(raw_data['medical']['medicFormat']):
                    res_data['药品规格'] = raw_data['medical']['medicFormat']

                if is_legal(raw_data['medical']['drugType']):
                    res_data['药品类型'] = raw_data['medical']['drugType']

                if is_legal(raw_data['medical']['thumbnail']):
                    res_data['药品图片'] = raw_data['medical']['thumbnail']

                if is_legal(raw_data['medical']['price']):
                    res_data['药品价格'] = raw_data['medical']['price']

                if is_legal(raw_data['medical']['functionalClassification']):
                    res_data['主要功效'] = raw_data['medical']['functionalClassification']

                if is_legal(raw_data['medical']['indication']):
                    res_data['适应病症'] = raw_data['medical']['indication']

                if is_legal(raw_data['medical']['dosage']):
                    res_data['用法用量'] = raw_data['medical']['dosage']

                if is_legal(raw_data['medical']['adverseReactions']):
                    res_data['不良反应'] = raw_data['medical']['adverseReactions']

                if is_legal(raw_data['medical']['taboo']):
                    res_data['禁忌'] = raw_data['medical']['taboo']

                if is_legal(raw_data['medical']['notice']):
                    res_data['禁忌'] = raw_data['medical']['notice']
                # 根据不同的分类爬取文档
                # result_document = parse_detail(url)

                # 将得到的文档插入Mongo数据库中
                insert_data(detail_collection, res_data)
                # 更新Url表中该Url的状态为 已爬取

        except BaseException as e:
            traceback.print_exc()
            err_obj = {'err_url': url, 'exception': repr(e)}
            insert_data("1_error", err_obj)
            all_console_log.logger.info(
                '错误信息：' + traceback.format_exc())
            err_console_log.logger.info(
                '错误信息：' + traceback.format_exc())
    pass


def parse():
    # 初始化全局变量
    globalVariable._init()
    # 读取爬取相关的配置文件
    crawler_config = read_crawler_config()

    thread_number = crawler_config['thread_number']

    # 每次请求的间隔休息时间
    sleep_time = crawler_config['sleep_time']
    retry_count = crawler_config['retry_count']
    if sleep_time.find(',') != -1:
        sleep_time = [int(sleep_time.split(',')[0]), int(sleep_time.split(',')[1])]
    else:
        sleep_time = int(sleep_time)
    globalVariable.set_value('sleep_time', sleep_time)
    globalVariable.set_value('retry_count', retry_count)
    # 读取MongoDB相关的配置文件
    mongo_config = read_mongo_config()
    globalVariable.set_value('mongo_config', mongo_config)

    all_console_log.logger.info(
        '已读取配置文件信息。')

    detail_collection = '1_medicine_detail_json'
    row_collection = '1_medicine_row_json'

    col = get_mongodb_collection(row_collection)
    raw_doc_iter = col.find()

    object_id_list = []
    for url_raw in raw_doc_iter:
        tmp = url_raw['_id']
        object_id_list.append(tmp)
    # 将所有Url装进队列，用于多线程爬取
    urlQueue = queue.Queue()
    for page in object_id_list:
        # url_queue.put(url_obj['url'])
        urlQueue.put(page)
    threads = []

    # 可以调节线程数， 进而控制抓取速度
    for i in range(0, thread_number):
        t = threading.Thread(target=parse_data_detail,
                             args=(urlQueue, row_collection,  detail_collection))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        # 多线程多join的情况下，依次执行各线程的join方法, 这样可以确保主线程最后退出， 且各个线程间没有阻塞
        t.join()




if __name__ == '__main__':
    if len(sys.argv) == 1 or len(sys.argv) > 2:
        print('参数错误，请输入-h查看详情')

    if len(sys.argv) == 2:
        parameter = sys.argv[1]
        if parameter == '-h' or parameter == '-help':
            print('药品数据采集系统 帮助:\n'
                  '-start: 开始执行爬虫\n'
                  '-check: 检查配置文件是否正确配置，同时检查爬虫代码是否正常运行')
            # TODO 这回我使用 先爬 再分析 的方式，所以多次执行，
            # TODO 运行时 在RUN/DEbug 里 Parameters 里面写下面的参数去执行

        elif parameter == '-start':
            schedule()
        elif parameter == '-geturl':
            geturl()
        elif parameter == '-getdetail':
            getdetail()
        elif parameter == '-parse':
            parse()
        else:
            print('参数错误，请输入-h查看详情')