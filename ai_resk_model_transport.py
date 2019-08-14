# coding:utf-8
import json
import pyodbc
from datetime import datetime
from functools import wraps
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import logging
import threading

import time
import os
import sys

project_list = []

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


def execute_many_sql_db2(strModel, listData):  # Fixme 修改为MYSQL兼容的
    try:
        conn, cursor = conn_db2()
        crs = conn.cursor()
        listData = [listData[i:i + 500] for i in range(0, len(listData), 500)]
        for child_listData in listData:
            crs.executemany(strModel, child_listData)
            conn.commit()
        crs.close()
        conn.close()
    except BaseException as e:
        GlobalLogging.getLog().info(e[0].decode('gbk'))


def execute_db2(sqlstr):
    conn, cursor = conn_db2()
    try:
        cursor.execute(sqlstr)
    except Exception as e:
        GlobalLogging.getLog().info(sqlstr)
        GlobalLogging.getLog().info(e)
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
def table_sx_cases():  # 操作sx_cases表，25万条

    sql = "select CASES_ID, CASE_NAME, PROJECT_CODE, CASE_LEVEL, CASE_TYPE, REMARKS " \
              "from TIMESVC.sx_cases"

    try:
        print u'正在加载sx_cases表数据，请稍等......'
        sx_cases_dict_list = execute_db2(sql)
        print u'sx_cases表数据加载完毕......'
        return sx_cases_dict_list  # sx_cases表数据已经取出
    except Exception as e:
        GlobalLogging.getLog().info(sql)
        GlobalLogging.getLog().info(e[0].decode('gbk'))


def get_sx_cases_detail_item(case_id):  # 40万条数据
    try:
        sql = "select INPUT_STRING, EXPECTED_OUTPUT from TIMESVC.sx_cases_detail where cases_id = '%s'" % case_id
        result_dict = execute_db2(sql)
        input_list = []
        expected_output = []
        for item in result_dict:
            input_list.append(item['INPUT_STRING'])
            expected_output.append(item['EXPECTED_OUTPUT'])
        for i in range(len(input_list)):  # 清洗INPUT_STRING脏数据
            if input_list[i] == None:
                input_list[i] = ''
        input_string = ';'.join(input_list)
        for j in range(len(expected_output)):  # 清洗EXPECTED_OUTPUT脏数据
            if expected_output[j] == None:
                expected_output[j] = ''
        expected_string = ';'.join(expected_output)
        return input_string, expected_string
    except Exception as e:
        GlobalLogging.getLog().info(sql)
        GlobalLogging.getLog().info(e[0].decode('gbk'))
        return '', ''


def get_df_project_information_new_v(project_code, result_dict):
    try:
        for item in result_dict:
            if project_code == item['P_REALCODE']:
                return item['GH_PROPERTY'], item['PT_PROPERTY'], \
                       item['GS_PROPERTY'], item['KF_PROPERTY'], \
                       item['ZYX'], item['GM']
        return '', '', '', '', '', ''
    except Exception as e:
        GlobalLogging.getLog().info(project_code)
        GlobalLogging.getLog().info(e[0].decode('gbk'))
        return '', '', '', '', '', ''


def delieve_all_data():
    sx_cases = table_sx_cases()  # 表sx_cases

    sql_info = "select P_REALCODE, GH_PROPERTY, PT_PROPERTY, GS_PROPERTY, KF_PROPERTY, ZYX, GM " \
               "from TIMESVC.DF_PROJECT_INFORMATION_NEW_V"
    result_dict = execute_db2(sql_info)

    list_data = [sx_cases[i:i + 5000] for i in range(0, len(sx_cases), 5000)]
    for sx_cases in list_data:
        insert_table_sx_ai_risk_train_data(sx_cases, result_dict)


@calculation_time
def insert_table_sx_ai_risk_train_data(sx_cases, result_dict):
    try:
        i = 0
        sql = """INSERT INTO TIMESVC.SX_AI_RISK_TRAIN_DATA
                            (
                                CASES_ID, 
                                CASE_NAME, 
                                PROJECT_CODE, 
                                CASE_LEVEL, 
                                CASE_TYPE, 
                                REMARKS, 
                                INPUT_STRING_SET, 
                                EXPECTED_OUTPUT_SET, 
                                GH_PROPERTY, 
                                PT_PROPERTY, 
                                GS_PROPERTY, 
                                KF_PROPERTY, 
                                ZYX, 
                                GM
                            ) VALUES (
                                ?, 
                                ?, 
                                ?, 
                                ?, 
                                ?, 
                                ?, 
                                ?, 
                                ?, 
                                ?, 
                                ?, 
                                ?, 
                                ?,
                                ?,
                                ?);"""

        sql_list = []

        for item in sx_cases:
            # sx_cases表处理完成
            try:
                case_id = item['CASES_ID']
                case_name = item['CASE_NAME']
                project_code = item['PROJECT_CODE']
                case_level = item['CASE_LEVEL']
                case_type = item['CASE_TYPE']
                remarks = item['REMARKS']

                # sx_cases_detail表
                input_string, expected_string = get_sx_cases_detail_item(case_id)  # 在此获取sx_cases_detail内部条目
                input_string_set = input_string
                expected_output_set = expected_string

                # df_project_information_new_v表
                gh_property, pt_property, gs_property, kf_property, zyx, gm = get_df_project_information_new_v(project_code, result_dict)

                sql_list.append((case_id, case_name, project_code, case_level, case_type, remarks, input_string_set,
                       expected_output_set, gh_property, pt_property, gs_property, kf_property, zyx, gm))
                # print sql
                i = i + 1
                if i % 100 == 0:
                    print u'已经处理: %d条数据' % i
            except Exception as e:
                GlobalLogging.getLog().info(e[0].decode('gbk'))

        execute_many_sql_db2(sql, sql_list)
    except Exception as e:
        GlobalLogging.getLog().info(e[0].decode('gbk'))


if __name__ == '__main__':
    delieve_all_data()
    # get_sx_cases_detail_item('016e4bed2caf41baadd14338ba106929')  # 脏数据案例

