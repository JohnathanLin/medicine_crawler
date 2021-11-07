#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/8/7 10:43
# @Author  : Johnathan Lin 凌虚
from bson import ObjectId
from bs4 import BeautifulSoup
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
def crawl_medicine_list(html_content):
    medicine_item_raw_list = html_content.select('.drug_item')
    medicine_item_list = []
    for medicine_item_raw in medicine_item_raw_list:
        a = 'https:'+medicine_item_raw.select('a')[0]['href']
        medicine_item_list.append(a)

    return medicine_item_list

def crawler(url):
    headers = create_headers()

    html = request(url, 'get', headers, None)
    # print(html)
    html_content = BeautifulSoup(html, 'lxml')
    fp_total = html_content.select('.fp_total')

    page_num = fp_total[0].get_text().lstrip('共').rstrip('件商品')
    res_list = []
    list = crawl_medicine_list(html_content)
    res_list.extend(list)
    print(res_list)
    return {'res_list': res_list}

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
            insert_data("5_error", err_obj)
            all_console_log.logger.info(
                '错误信息：' + traceback.format_exc())
            err_console_log.logger.info(
                '错误信息：' + traceback.format_exc())



def get_document(page_num, thread_number, source_collection_name, target_collection_name):
    """
    爬取文档
    :param classify_id: 分类ID
    :param batch_number: 批次号
    :param thread_number: 线程数
    :return:
    """
    # 获取省份和城市信息
    col = get_mongodb_collection(source_collection_name)
    doc = col.find_one({'_id': ObjectId('5f360319ec96dae16bf5901e')})
    # 将所有Url装进队列，用于多线程爬取
    urlQueue = queue.Queue()
    for page in doc['category_list']:
        # url_queue.put(url_obj['url'])
        url = page
        # print(url)
        urlQueue.put(url)
    threads = []


    # 可以调节线程数， 进而控制抓取速度
    for i in range(0, thread_number):
        t = threading.Thread(target=crawl_data,
                             args=( urlQueue, target_collection_name))
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

    url_mongo_collection = "5_url"
    mongo_url_detail_collection = '5_url_detail'
    col = get_mongodb_collection(url_mongo_collection)
    url_raw_doc = col.find()

    headers = create_headers()
    headers['Cookie'] = '__jsluid_s=57d13d5fa59332561927a6671423a004; UM_distinctid=173d856b2f3532-08bc524cfa5d06-87f133f-1fa400-173d856b2f4917; historysearch=; __jsluid_h=4ccc700b04db95a233351430ecc359e5; real_ip=58.23.15.115; Hm_lvt_e5f454eb1aa8e839f8845470af4667eb=1597062427,1597369731; hotkeywords=999%23%230%23%230%23%23https%3A%2F%2Fwww.yaofangwang.com%2Fsearch%2F13791.html%40%40%E7%89%87%E4%BB%94%E7%99%80%23%230%23%230%23%23https%3A%2F%2Fwww.yaofangwang.com%2Fsearch%2F39735.html%40%40%E9%98%BF%E8%83%B6%23%231%23%230%23%23https%3A%2F%2Fwww.yaofangwang.com%2Fsearch%2F11442.html%40%40%E9%87%91%E6%88%88%23%230%23%230%23%23https%3A%2F%2Fwww.yaofangwang.com%2Fsearch%2F30642.html%40%40%E6%B1%A4%E8%87%A3%E5%80%8D%E5%81%A5%23%230%23%230%23%23https%3A%2F%2Fwww.yaofangwang.com%2Fsearch%2F50493.html; CNZZDATA1261831897=1114880938-1597059686-%7C1597369749; Hm_lpvt_e5f454eb1aa8e839f8845470af4667eb=1597369750'

    res_list = []
    for url_raw in url_raw_doc:
        html_content = BeautifulSoup(url_raw['html'], 'lxml')
        # 基础信息
        [s.extract() for s in html_content.select('style')]
        [s.extract() for s in html_content.select('script')]

        id_list = []
        div_list = html_content.select('div.medicineInfo')

        url = 'https://www.yaofangwang.com/Medicine/getMedicineByIds?mids='

        for index, div in enumerate(div_list):
            # print(a['href'], a.get_text())

        # list = response_obj['list']
        # for medicine in list:
        #
            id_list.append(div['rel'])

        json = request(url + ",".join(id_list), 'get', headers, None)
        print(json)
        res_obj = {'json': json}
        insert_data(mongo_url_detail_collection, res_obj)

#== 读取失败的记录 ==#
    # url_mongo_collection = "5_error"
    # mongo_url_detail_collection = '5_url_detail'
    # col = get_mongodb_collection(url_mongo_collection)
    # url_raw_doc = col.find()
    #
    # res_list = []
    # for url_raw in url_raw_doc:
    #     detail_mongo_obj = {'url': url_raw['err_url']}
    #     res_list.append(detail_mongo_obj)
    #
    # res_obj = {'url_list': res_list}
    # insert_data(mongo_url_detail_collection, res_obj)

#===============================#

def crawler_detail(url):
    html = request(url, 'get', create_headers(), None)
    obj = {'raw_html': html}
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
            insert_data("5_error", err_obj)
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
    url_list = []

    mongo_url_detail_collection = '5_url'
    doc_list = get_mongodb_collection(mongo_url_detail_collection).find()
    for doc in doc_list:

        doc_url_list = doc['url_list']
        url_list.extend(doc_url_list)
    collection_name = "5_medicine_row_html"
    # print(len(url_list))
    # 将所有Url装进队列，用于多线程爬取
    urlQueue = queue.Queue()
    for page in url_list:
        # url_queue.put(url_obj['url'])

        urlQueue.put(page)
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

    url_mongo_collection = "5_category"
    page_num = 587
    get_document(page_num, thread_number, url_mongo_collection, "5_url")


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

def getcategory():
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
    url = 'https://www.yaofang.cn/c/autokey/categoryAll'
    html = request(url, 'get', create_headers(), None)
    # print(html)
    html_content = BeautifulSoup(html, 'lxml')
    a_list = html_content.select('.juTi')[0].select('a')
    category_list = []
    for a in a_list:

        category_list.append('https:'+a['href'])
    res_obj = {'category_list': category_list}
    insert_data('5_category', res_obj)


if __name__ == '__main__':
    if len(sys.argv) == 1 or len(sys.argv) > 2:
        print('参数错误，请输入-h查看详情')

    if len(sys.argv) == 2:
        parameter = sys.argv[1]
        if parameter == '-h' or parameter == '-help':
            print('药品数据采集系统 帮助:\n'
                  '-start: 开始执行爬虫\n'
                  '-check: 检查配置文件是否正确配置，同时检查爬虫代码是否正常运行')
        elif parameter == '-getcategory':
            getcategory()
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