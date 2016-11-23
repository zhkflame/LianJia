# encoding: utf-8

from ConnMysql import MySQLWraper
from BeautifulSoup import *
import urllib2
import random
import threading

import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

regions=[u"东城",u"西城",u"朝阳",u"海淀",u"丰台",u"石景山","通州",u"昌平",u"大兴",u"亦庄开发区",u"顺义",u"房山",u"门头沟",u"平谷",u"怀柔",u"密云",u"延庆",u"燕郊"]
conf=['localhost','root','123','lianjia']
#Some User Agents
hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

def do_xiaoqu_spider(db_xq,region=u"昌平"):
    """
    爬取大区域中的所有小区信息
    """
    url=u"http://bj.lianjia.com/xiaoqu/rs"+region+"/"
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)]) #指定一个域名并发送请求
        page_obj = urllib2.urlopen(req,timeout=10)  #服务端响应来自客户端的请求,获取网页对象
        source_code=page_obj.read() #读取网页结构内容
        plain_text=unicode(source_code) #将string按照encoding的格式编码成为unicode对象
        soup = BeautifulSoup(plain_text) #通过BeautifulSoup包装内容
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        return
    except Exception,e:
        print e
        return
    d="d="+soup.find('div',{'class':'page-box house-lst-page-box'}).get('page-data')
    #查找class为page-box house-lst-page-box的div，并获取其中的page-data内容
    exec(d) #执行（）内语句，是的d等于一个字典
    total_pages=d['totalPage']
    #print total_pages

    threads=[]
    for i in range(total_pages):
        url_page=u"http://bj.lianjia.com/xiaoqu/pg%drs%s/" % (i+1,region) #拼接相应的网址
        t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
        #根据页码数，创建对应的线程去爬去各页面内容
        threads.append(t)
    # 启动所有线程
    for t in threads:
        t.start()
    # 主线程中等待所有子线程退出
    for t in threads:
        t.join()
    print u"爬下了 %s 区全部的小区信息" % region


def xiaoqu_spider(db_xq,url_page=u"http://bj.lianjia.com/xiaoqu/pg1rs%E6%98%8C%E5%B9%B3/"):
    """
    爬取页面链接中的小区信息
    """
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text)
        #print soup
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exit(-1)
    except Exception,e:
        print e
        exit(-1)

    xiaoqu_list=soup.findAll('div',{'class':'info'})
    print xiaoqu_list
    for xq in xiaoqu_list:
        info_dict={}
        info_dict.update({u'小区名称':xq.find('a').text})
        print xq
        content=unicode(xq.find('div',{'class':'houseInfo'}).renderContents().strip())
        info=re.match(r".+>(.+)</a>.+>(.+)</a>.+</span>(.+)<span>.+</span>(.+)",content)
        if info:
            info=info.groups()
            info_dict.update({u'大区域':info[0]})
            info_dict.update({u'小区域':info[1]})
            info_dict.update({u'小区户型':info[2]})
            info_dict.update({u'建造时间':info[3][:4]})
        command=gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command,1)

def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=[u'小区名称',u'大区域',u'小区域',u'小区户型',u'建造时间']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?)",t)
    print command
    return command


if __name__=="__main__":

    command1="create table If Not Exists xiaoqu(name VARCHAR(50) primary key, regionb VARCHAR(50), regions VARCHAR(50), style VARCHAR(50), year VARCHAR(50))"
    db_xq=MySQLWraper(conf,command1)

    command2="create table if not exists chengjiao (href VARCHAR(50) primary key, name VARCHAR(50), " \
             "style VARCHAR(50), area VARCHAR(50), orientation VARCHAR(50), floor VARCHAR(50), " \
             "year VARCHAR(50), sign_time VARCHAR(50), unit_price VARCHAR(50), total_price VARCHAR(50)," \
             "fangchan_class VARCHAR(50), school VARCHAR(50), subway VARCHAR(50))"
    db_cj=MySQLWraper(conf,command2)


    #爬下所有的小区信息
    for region in regions:
        do_xiaoqu_spider(db_xq,region)

    #爬下所有小区里的成交信息
    #do_xiaoqu_chengjiao_spider(db_xq,db_cj)

    #重新爬取爬取异常的链接
    #exception_spider(db_cj)