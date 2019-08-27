# coding:utf-8
import json
import pyodbc
from datetime import datetime
from functools import wraps

import logging
import threading

import time
import os
import re
import csv

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

g_traceback_template = '''Traceback (most recent call last):  File "%(filename)s", line %(lineno)s, in %(name)s %(type)s: %(message)s %(more_info)s\n'''  # Skipping the "actual line" item


class NullHandler(logging.Handler):
    def emit(self, record): pass


class GlobalLogging:
    log = None
    root_hundlers = 0
    ch_handler = ""
    fh_handler = ""

    @staticmethod
    def getLog():

        homepath = os.getcwd()
        log_path = os.path.join(homepath, "logs")
        if not os.path.isdir(log_path):
            os.mkdir(log_path)
        date = time.strftime("%Y%m%d", time.localtime())
        log_path = os.path.join(log_path, "ai_resk_%s.log" % date)
        if GlobalLogging.log != None:
            if not os.path.exists(log_path):
                GlobalLogging.root_hundlers = 1
                GlobalLogging.log = None
        if GlobalLogging.log == None:
            GlobalLogging.log = GlobalLogging()

        return GlobalLogging.log

    def __init__(self):
        self.logger = None
        self.handler = None
        self.level = logging.DEBUG
        self.logger = logging.getLogger("GlobalLogging")
        self.formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        h = NullHandler()
        self.logger.addHandler(h)

        # fixme
        self.setLoggingLevel(self.level)  # 必须设置，否则出不来
        self.setLoggingToConsole()

        date = time.strftime("%Y%m%d", time.localtime())

        if getattr(sys, 'frozen', False):
            application_path = os.path.dirname(sys.executable)
        elif __file__:
            application_path = os.path.dirname(__file__)

        log_path = os.path.join(application_path, "logs")
        if not os.path.isdir(log_path):
            os.mkdir(log_path)
        log_path = os.path.join(log_path, "ai_resk_%s.log" % date)

        self.setLoggingToFile(log_path)

        self.lock = threading.Lock()

    def setLoggingToFile(self, file):
        if GlobalLogging.root_hundlers == 1:
            self.logger.removeHandler(GlobalLogging.fh_handler)
        fh = logging.FileHandler(file)
        GlobalLogging.fh_handler = fh
        fh.setFormatter(self.formatter)
        fh.setLevel(self.level)
        self.logger.addHandler(fh)

    def setLoggingToConsole(self):
        if GlobalLogging.root_hundlers == 1:
            self.logger.removeHandler(GlobalLogging.ch_handler)
        ch = logging.StreamHandler()
        GlobalLogging.ch_handler = ch
        ch.setFormatter(self.formatter)
        ch.setLevel(self.level)
        self.logger.addHandler(ch)

    def setLoggingToHandler(self, handler):
        self.handler = handler

    def setLoggingLevel(self, level):
        self.level = level
        self.logger.setLevel(level)

    def debug(self, s):
        s = "%s:%s " % (sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno) + s
        self.logger.debug(s)
        if not self.handler == None and self.level <= logging.DEBUG:
            self.handler('-DEBUG-:' + s)

    def info(self, s):
        self.lock.acquire()
        s = "%s:%s " % (sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno) + s
        self.logger.info(s)
        if not self.handler == None and self.level <= logging.INFO:
            self.handler('-INFO-:' + s)
        self.lock.release()

    def warn(self, s):
        self.lock.acquire()
        s = "%s:%s " % (sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno) + s
        self.logger.warn(s)
        if not self.handler == None and self.level <= logging.WARNING:
            self.handler('-WARN-:' + s)
        self.lock.release()

    def error(self, s):
        self.lock.acquire()
        s = "%s:%s " % (sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno) + s
        self.logger.error(s)
        if not self.handler == None and self.level <= logging.ERROR:
            self.handler('-ERROR-:' + s)
        self.lock.release()

    def critical(self, s):
        self.lock.acquire()
        s = "%s:%s" % (sys._getframe().f_back.f_code.co_name, sys._getframe().f_back.f_lineno) + s
        self.logger.critical(s)
        if not self.handler == None and self.level <= logging.CRITICAL:
            self.handler('-CRITICAL-:' + s)
        self.lock.release()


# 因select数据量较大，计算i/o时间，进行下一步分析
def calculation_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        print u'共耗时:%s秒' % str((end_time - start_time).seconds)
        return result
    return wrapper


def read_config():  # 打开配置文件
    try:
        f = open("settings.json", 'r')
        f_data = json.load(fp=f, encoding='utf-8')
        return f_data
    except Exception as e:
        GlobalLogging.getLog().info(e[0].decode('gbk'))


def conn_db2():  # 连接db2

    database_info_dict = read_config()

    try:
        dbstring = u'driver={IBM DB2 ODBC DRIVER};' \
                   u'database=%s;' \
                   u'hostname=%s;' \
                   u'port=%s;' \
                   u'protocol=tcpip;' \
                   u'uid=%s;' \
                   u'pwd=%s;' \
                   u'LONGDATACOMPAT=1;' \
                   u'LOBMAXCOLUMNSIZE=10485875;' % (database_info_dict['database'],
                                                    database_info_dict['ip'],
                                                    database_info_dict['port'],
                                                    database_info_dict['username'],
                                                    database_info_dict['password'])
        conn = pyodbc.connect(dbstring)
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        GlobalLogging.getLog().info(e[0].decode('gbk'))


def execute_db2(sqlstr):
    conn, cursor = conn_db2()
    try:
        cursor.execute(sqlstr)
    except Exception as e:
        GlobalLogging.getLog().info(sqlstr)
        GlobalLogging.getLog().info(e[0].decode('gbk'))
    ds = []
    if sqlstr.lower().__contains__("update") or sqlstr.lower().__contains__("insert") or sqlstr.lower().__contains__(
            "delete") or sqlstr.lower().__contains__("replace"):
        cursor.commit()
    else:
        rows = cursor.fetchall()
        ds = []

        for row in rows:
            rec = {}
            for i, desc in enumerate(row.cursor_description):
                if isinstance(row[i], str):
                    rec[desc[0]] = row[i].decode('gbk')
                else:
                    rec[desc[0]] = row[i]
            if rec != None:
                ds.append(rec)
    conn.commit()
    cursor.close()
    conn.close()
    return ds


@calculation_time
def table_sx_project():  # 操作sx_cases表，25万条

    sql = "SELECT PROJECT_NAME, P_SYS_OWN FROM AUTOTEST.SX_PROJECT"
    try:
        print u'正在加载sx_project表数据，请稍等......'
        sx_cases_dict_list = execute_db2(sql)
        print u'sx_project表数据加载完毕......'
        return sx_cases_dict_list  # sx_cases表数据已经取出
    except Exception as e:
        GlobalLogging.getLog().info(sql)
        GlobalLogging.getLog().info(e[0].decode('gbk'))


def exec_p_sys_own(p_str):
    try:
        ret_list = []
        pattern = '[,;]+'
        p_tuple = tuple(set(re.split(pattern, p_str)))
        for i in p_tuple:
            j = "'" + str(i) + "'"
            ret_list.append(j)
        ret_list = ','.join(ret_list)
        return ret_list
    except Exception as e:
        GlobalLogging.getLog().info(e[0].decode('gbk'))


def exec_sx_project_data():
    try:
        pro_info_list = table_sx_project()
        if os.path.exists('rela_cases.csv'):
            os.remove('rela_cases.csv')
        with open('rela_cases.csv', 'a+') as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')
            csvWriter.writerow([u'项目名', u'所属系统', u'系统ID'])
        for item in pro_info_list:
            try:
                pro_name = item['PROJECT_NAME']
                p_sys_own = item['P_SYS_OWN']
                if p_sys_own:
                    p_sys_right_str = exec_p_sys_own(p_sys_own)  # 将p_sys_own处理成正确的格式
                    sql = "select ID, CFNAME from AUTOTEST.DF_CMDB_V where ID in (" + p_sys_right_str + ")"
                    sx_df_cmdb = execute_db2(sql)
                    for item in sx_df_cmdb:
                        ele_list = []
                        ele_list.append(pro_name)
                        ele_list.append(item['CFNAME'])
                        ele_list.append(item['ID'])
                        with open('rela_cases.csv', 'a+') as csvFile:
                            csvWriter = csv.writer(csvFile, delimiter=',')
                            csvWriter.writerow(ele_list)
                    print ele_list
                else:
                    pro_list = []
                    pro_list.append(pro_name)
                    with open('rela_cases.csv', 'a+') as csvFile:
                        csvWriter = csv.writer(csvFile)
                        csvWriter.writerow(pro_list)
            except Exception as e:
                GlobalLogging.getLog().info(e[0].decode('gbk'))
    except Exception as e:
        GlobalLogging.getLog().info(e[0].decode('gbk'))


if __name__ == '__main__':
    # str = "1354857471267P58XV,14289094079862AUJ1;;1354857471267P58XV,14289094079862AUJ1,1428909813393D3OJB;;1354857471267P58XV,14289094079862AUJ1,14289097621110EI8X;;1354857471267P58XV,14289094079862AUJ1,1428909511174ACRWN;;1354857471267P58XV,14289094079862AUJ1,1428909491330M7PS9;;1354857471267P58XV,14289094079862AUJ1,1428909456940JZ060;;1354857471267P58XV,14289094079862AUJ1,1428910015486GSKP9;;1354857471267P58XV,14289094079862AUJ1,1428909882346KOS19"
    # exec_p_sys_own(str)
    exec_sx_project_data()





















