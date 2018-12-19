# -*- coding:gbk -*-
import csv
import json
import pyodbc

import datetime

import sys

import os

import time

reload(sys)
sys.setdefaultencoding('gbk')


def open_json_file(json_file_name):  # 打开配置json文件
    try:
        f = open(json_file_name).read()
        # print f
        conn_str = json.loads(f)
        print u'打开json文件成功'
        return conn_str
    except Exception as e:
        print u'打开配置json文件异常'


def ConnectToRunDB(conn_str):  # 通过json文件连接环境需要备份的环境数据库
    '''
    获取待对比表数据
    :return:
    '''
    print u'连接sqlserver数据库成功'
    try:
        dbstring = "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;autocommit=True" % (
            conn_str['ip'], conn_str['database'], conn_str['username'], conn_str['password'])
        conn = pyodbc.connect(dbstring)
        return conn
    except Exception as e:
        print u'通过json文件连接环境数据库异常'


def foucs_core(conn_str):  # 定位核心号，core_id核心号
    print u'开始定位核心号'
    try:
        chosen_key = conn_str['chosen_key']
        for i in conn_str:
            if chosen_key == i and i != "chosen_key":
                conn = ConnectToRunDB(conn_str[i])
                print u'定位核心号成功', i
                return conn

    except Exception as e:
        print u'定位核心号异常'


def write_csv(file_name, data):  # 将数据写入csv文件 --> 为tran_data_to_csv独立出的方法
    print u'正在将一份{}数据表数据写入csv文件'.format(len(data)), file_name
    try:
        with open(file_name, 'ab+') as csvFile:
            csvWriter = csv.writer(csvFile)
            for field in data:
                csvWriter.writerow(field)
                # print u'已写入{}'.format(field)
                del field
            print u'已经插入{}'.format(len(data))
            csvFile.close()
    except Exception as e:
        print u'将一份(200000)数据表数据写入csv文件出错', e


def tran_data_to_csv(_table, counts, globe_json, table, order_key):  # 将数据备份至csv文件
    print u'准备数据表全数写入'
    try:
        for count in range(counts):
            conn = foucs_core(globe_json)
            cursor = conn.cursor()
            sql_str = """ select * from %s order by %s offset %s rows fetch next 200000 rows only""" % (table, order_key, count*200000)
            # sql_str = """select * from [run].[dbo].[stkasset]"""
            # 每10000行进行备份一次
            print u'开始进行备份: {}-->{}条数据，sql：{}'.format(count*200000, (count+1)*200000, sql_str)
            cursor.execute(sql_str)
            ds = cursor.fetchall()
            if ds[0]:
                table_name = _table + '.csv'
                write_csv(table_name, ds)
                # del ds  # 是否可以在内存中进行删除，需进行验证处理
                cursor.close()
                conn.close()
            else:
                print u'此sql查无值', sql_str
        print u'数据表全数写入完成'
    except Exception as e:
        print u'准备数据表全数写入异常 --> {}表内没有数据'.format(table)


def count_back_up_range(globe_json, table):  # 因数据量较大，需要分几次从数据库中进行查找
    print u'准备数据量切分'
    conn = foucs_core(globe_json)  # 从配置文件锁定需要备份的核心号，并拿到此核心号的数据库连接游标
    cursor = conn.cursor()

    count_str = """select count(*) from %s""" % table
    cursor.execute(count_str)
    counts = cursor.fetchall()[0][0]
    counts = counts/200000 + 1
    print u'数据量切分完成', counts
    return counts


def write_title_to_csv(_table, globe_json, table):  # 将表结构写入csv文件

    conn = foucs_core(globe_json)  # 从配置文件锁定需要备份的核心号，并拿到此核心号的数据库连接游标
    cursor = conn.cursor()
    print u'csv文件开始写入表结构头部 ------------------------------> ', table
    sql = "select top 1 * from %s" % table
    print u'表头sql_str --> ', sql
    cursor.execute(sql)
    rows = cursor.fetchall()
    rs_list = []
    for row in rows:
        for i, desc in enumerate(row.cursor_description):
            rs_list.append(desc[0])
    try:
        table_name = _table + '.csv'
        with open(table_name, 'ab+') as csvFile:
            csvWriter = csv.writer(csvFile)
            csvWriter.writerow(rs_list)
            csvFile.close()

    except Exception as e:
        print u'dbf转换csv出错', e

    print u'表结构字段写入成功'

    cursor.close()
    conn.close()


def get_table_list(globe_json):

    back_up_key = globe_json['chosen_key']

    foucs_core_list = globe_json[back_up_key]["back_up_database"]

    return foucs_core_list


def globe_control(_table, globe_json, table, order_key, csv_path):

    _file_path = csv_path + '\\' + _table

    write_title_to_csv(_file_path, globe_json, table)

    counts = count_back_up_range(globe_json, table)  # 进行csv表头的备份

    tran_data_to_csv(_file_path, counts, globe_json, table, order_key)  # 进行csv内容的备份


def get_path_name(globe_json):

    path_name = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    core = 'core_' + globe_json['chosen_key'] + '_'

    path = core + path_name

    if not os.path.exists(path):
        os.makedirs(path)

    return path


def exec_back_up(SETTINGS_FILE):  # 控制整体进度函数

    begin_time = datetime.datetime.now()

    globe_json = open_json_file(SETTINGS_FILE['env_json_file'])  # 拿到配置文件的整体json

    csv_path = get_path_name(globe_json)

    table_list = get_table_list(globe_json)  # 拿到备份表的列表(此核心内所有需要备份的表)


    print '需要备份的所有表项 table_list --> ', table_list

    back_up_status = globe_json['back_up_status']

    for table in table_list:

        table_name = table + back_up_status

        table_true = table

        order_key = table_list[table]

        print u'globe_control 已经开始'

        globe_control(table_name, globe_json, table_true, order_key, csv_path)

        print u'{}globe_control 已经结束'.format(table_list[table])

    end_time = datetime.datetime.now()

    detla_time = end_time - begin_time

    print u'共用时 --> ', detla_time.seconds


if __name__ == '__main__':

    SETTINGS_FILE = {
        'env_json_file': 'env_settings.json',
    }

    exec_back_up(SETTINGS_FILE)

