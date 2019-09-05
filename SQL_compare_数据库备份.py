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


def open_json_file(json_file_name):  # ������json�ļ�
    try:
        f = open(json_file_name).read()
        # print f
        conn_str = json.loads(f)
        print u'��json�ļ��ɹ�'
        return conn_str
    except Exception as e:
        print u'������json�ļ��쳣'


def ConnectToRunDB(conn_str):  # ͨ��json�ļ����ӻ�����Ҫ���ݵĻ������ݿ�
    '''
    ��ȡ���Աȱ�����
    :return:
    '''
    print u'����sqlserver���ݿ�ɹ�'
    try:
        dbstring = "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s;autocommit=True" % (
            conn_str['ip'], conn_str['database'], conn_str['username'], conn_str['password'])
        conn = pyodbc.connect(dbstring)
        return conn
    except Exception as e:
        print u'ͨ��json�ļ����ӻ������ݿ��쳣'


def foucs_core(conn_str):  # ��λ���ĺţ�core_id���ĺ�
    print u'��ʼ��λ���ĺ�'
    try:
        chosen_key = conn_str['chosen_key']
        for i in conn_str:
            if chosen_key == i and i != "chosen_key":
                conn = ConnectToRunDB(conn_str[i])
                print u'��λ���ĺųɹ�', i
                return conn

    except Exception as e:
        print u'��λ���ĺ��쳣'


def write_csv(file_name, data):  # ������д��csv�ļ� --> Ϊtran_data_to_csv�������ķ���
    print u'���ڽ�һ��{}���ݱ�����д��csv�ļ�'.format(len(data)), file_name
    try:
        with open(file_name, 'ab+') as csvFile:
            csvWriter = csv.writer(csvFile)
            for field in data:
                csvWriter.writerow(field)
                # print u'��д��{}'.format(field)
                del field
            print u'�Ѿ�����{}'.format(len(data))
            csvFile.close()
    except Exception as e:
        print u'��һ��(200000)���ݱ�����д��csv�ļ�����', e


def tran_data_to_csv(_table, counts, globe_json, table, order_key):  # �����ݱ�����csv�ļ�
    print u'׼�����ݱ�ȫ��д��'
    try:
        for count in range(counts):
            conn = foucs_core(globe_json)
            cursor = conn.cursor()
            sql_str = """ select * from %s order by %s offset %s rows fetch next 200000 rows only""" % (table, order_key, count*200000)
            # sql_str = """select * from [run].[dbo].[stkasset]"""
            # ÿ10000�н��б���һ��
            print u'��ʼ���б���: {}-->{}�����ݣ�sql��{}'.format(count*200000, (count+1)*200000, sql_str)
            cursor.execute(sql_str)
            ds = cursor.fetchall()
            if ds[0]:
                table_name = _table + '.csv'
                write_csv(table_name, ds)
                # del ds  # �Ƿ�������ڴ��н���ɾ�����������֤����
                cursor.close()
                conn.close()
            else:
                print u'��sql����ֵ', sql_str
        print u'���ݱ�ȫ��д�����'
    except Exception as e:
        print u'׼�����ݱ�ȫ��д���쳣 --> {}����û������'.format(table)


def count_back_up_range(globe_json, table):  # ���������ϴ���Ҫ�ּ��δ����ݿ��н��в���
    print u'׼���������з�'
    conn = foucs_core(globe_json)  # �������ļ�������Ҫ���ݵĺ��ĺţ����õ��˺��ĺŵ����ݿ������α�
    cursor = conn.cursor()

    count_str = """select count(*) from %s""" % table
    cursor.execute(count_str)
    counts = cursor.fetchall()[0][0]
    counts = counts/200000 + 1
    print u'�������з����', counts
    return counts


def write_title_to_csv(_table, globe_json, table):  # ����ṹд��csv�ļ�

    conn = foucs_core(globe_json)  # �������ļ�������Ҫ���ݵĺ��ĺţ����õ��˺��ĺŵ����ݿ������α�
    cursor = conn.cursor()
    print u'csv�ļ���ʼд���ṹͷ�� ------------------------------> ', table
    sql = "select top 1 * from %s" % table
    print u'��ͷsql_str --> ', sql
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
        print u'dbfת��csv����', e

    print u'��ṹ�ֶ�д��ɹ�'

    cursor.close()
    conn.close()


def get_table_list(globe_json):

    back_up_key = globe_json['chosen_key']

    foucs_core_list = globe_json[back_up_key]["back_up_database"]

    return foucs_core_list


def globe_control(_table, globe_json, table, order_key, csv_path):

    _file_path = csv_path + '\\' + _table

    write_title_to_csv(_file_path, globe_json, table)

    counts = count_back_up_range(globe_json, table)  # ����csv��ͷ�ı���

    tran_data_to_csv(_file_path, counts, globe_json, table, order_key)  # ����csv���ݵı���


def get_path_name(globe_json):

    path_name = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    core = 'core_' + globe_json['chosen_key'] + '_'

    path = core + path_name

    if not os.path.exists(path):
        os.makedirs(path)

    return path


def exec_back_up(SETTINGS_FILE):  # ����������Ⱥ���

    begin_time = datetime.datetime.now()

    globe_json = open_json_file(SETTINGS_FILE['env_json_file'])  # �õ������ļ�������json

    csv_path = get_path_name(globe_json)

    table_list = get_table_list(globe_json)  # �õ����ݱ���б�(�˺�����������Ҫ���ݵı�)


    print '��Ҫ���ݵ����б��� table_list --> ', table_list

    back_up_status = globe_json['back_up_status']

    for table in table_list:

        table_name = table + back_up_status

        table_true = table

        order_key = table_list[table]

        print u'globe_control �Ѿ���ʼ'

        globe_control(table_name, globe_json, table_true, order_key, csv_path)

        print u'{}globe_control �Ѿ�����'.format(table_list[table])

    end_time = datetime.datetime.now()

    detla_time = end_time - begin_time

    print u'����ʱ --> ', detla_time.seconds


if __name__ == '__main__':

    SETTINGS_FILE = {
        'env_json_file': 'env_settings.json',
    }

    exec_back_up(SETTINGS_FILE)

