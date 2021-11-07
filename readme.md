## 爬取药品数据
### 网站列表：
1. 中国医药信息查询平台 https://www.dayi.org.cn/list/4
2. 福药堂 http://www.fuyaotang.com/d_147605
3. 药房网商城 https://www.yaofangwang.com/
4. 华佗药房 http://www.huatuoyf.com/
5. 药房网 https://www.yaofang.cn/
6. 好药师 https://www.ehaoyao.com/index.html

### 配置文件
在执行爬虫之前需要设置爬虫执行时的配置，与爬取的数据保存的位置。

#### 爬虫配置
在项目根目录下的/config/crawler.ini可以设置以下内容
```
[THREADNUMBER]
threadNumber=5

[SLEEPTIME]
sleepTime=0

[RETRYCOUNT]
retryCount=20
```
> 爬虫配置文件字段解释：
> 
> threadNumber: 线程数
> 
> sleepTime: 两次请求之间的睡眠时间
> 
> retryCount: 重试次数


#### MongoDB配置
```
[CONFIG]
client=mongodb://192.168.46.120:20000/
auth=True
username=znkf_raw
password=znkf_raw123
database=bigdata_znkf_crawl_raw
```
> MongoDB配置文件字段解释：
> 
> client: 爬虫服务器
> 
> auth: 是否需要账号密码
> 
> username: 用户名（如auth为False则可为空）
> 
> password: 密码（如auth为False则可为空）
> 
> database: 具体要存入的数据库


### 启动爬虫
在项目根目录的命令行模式下，执行以下命令开始爬虫：
```bash
python scheduler.py -start
```
执行以下命令查看当前允许的参数
```
python scheduler.py -h
```
