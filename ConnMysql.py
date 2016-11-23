# encoding: utf-8
import MySQLdb
import threading
import time
import random
from time import ctime,sleep

class MySQLWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,conf,command='',*args,**kwargs):
        self.lock = threading.RLock() #锁
        self.host = conf[0] #数据库连接参数
        self.user=conf[1]
        self.passwd=conf[2]
        self.db=conf[3]

        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)
            conn.commit()

    def get_conn(self):
        conn = MySQLdb.connect(self.host,self.user,self.passwd,self.db,charset='utf8')#,check_same_thread=False)
        #conn.text_factory=str
        #print conn.text_factory
        return conn

    def close(self,conn=None):
        conn.close()

    def conn_trans(func):
        def connection(self,*args,**kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self,*args,**kwargs)
            self.close(conn)
            self.lock.release()
            return rs
        return connection

    @conn_trans
    def execute(self,command,method_flag=0,conn=None):
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except MySQLdb.IntegrityError,e:
            print e
            return -1
        except Exception, e:
            print e
            return -2
        return 0

    @conn_trans
    def fetchall(self,command="select * from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception,e:
            print e
            pass
        return lists

conf=['localhost','root','123','lianjia']
command="create table If Not Exists xiaoqu2(name VARCHAR(50) primary key, friend VARCHAR(50), abc VARCHAR(50), def VARCHAR(50))"
#command1="select * from xiaoqu"
#res=a.fetchall(command1)

def music(str):
    a=MySQLWraper(conf,command)
    for i in range(2):
        a.execute("insert into xiaoqu2 VALUES ('zhk',%s,'love',%s)" %(str,i))
        print 'music'
        time.sleep(random.randrange(3))

def move(str):
    b=MySQLWraper(conf)
    for i in range(2):
        b.execute("insert into xiaoqu2 VALUES ('wwz',%s,'love',%s)" %(str,i+2))
        print 'move'
        time.sleep(random.randrange(3))

if __name__ == '__main__':
    threads=[]
    t1=threading.Thread(target=music,args=(u"'爱情买'",))
    threads.append(t1)
    t2=threading.Thread(target=move,args=(u"'阿凡达'",))
    threads.append(t2)
    for t in threads:
        t.setDaemon(True)
        t.start()
    for t in threads:
        t.join()
    print "all over %s" %time.ctime()