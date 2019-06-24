# coding=utf-8
import csv
import datetime

import chardet
import pandas as pd
from collections import OrderedDict
import threading
import json


class BdfTask(threading.Thread):
    def __init__(self, DBF_FILE_NAME, NO_CHECK_PARAM, primary):
        threading.Thread.__init__(self)
        self.DBF_FILE_NAME = DBF_FILE_NAME
        self.NO_CHECK_PARAM = NO_CHECK_PARAM
        self.primary_key = primary

    def judge_primary(self, PRIMARY_KEY, values_dbf1, values_dbf2):
        # for i in range(len(PRIMARY_KEY)):
        #     if values_dbf2[PRIMARY_KEY[i]] == values_dbf1[PRIMARY_KEY[i]]:
        #         judge_primary.append('1')
        #     else:
        #         judge_primary.append('0')
        #
        # if '0' in ''.join(judge_primary):
        #     k = 0
        # else:
        #     k = 1
        first_key = ''
        last_key = ''
        for i in PRIMARY_KEY:
            first_key = str(values_dbf1[i]) + first_key
            last_key = str(values_dbf2[i]) + last_key
        if first_key == last_key:
            k = 1
        else:
            k = 0
        return k

    def get_primary_key(self, csv_list, primary_key):
        key_list = []
        for i in range(len(csv_list)):
            for j in range(len(primary_key)):
                if csv_list[i] == primary_key[j]:
                    key_list.append(j)
        return key_list

    def dbf2_opr(self, values_dbf1, values_dbf2, PRIMARY_KEY, bxt, table_stru):  # 具体差异细节
        try:
            diff_dict = OrderedDict()
            # print u'     正在进行进一步行列比对......', values_dbf1[0:5], values_dbf2[0:5]
            for key_dbf1 in range(len(values_dbf1)):
                for key_dbf2 in range(len(values_dbf2)):
                    if bxt:
                        if table_stru[key_dbf2] in bxt or table_stru[key_dbf1] in bxt:
                            continue
                    else:
                        pass
                    if key_dbf2 == key_dbf1:
                        if str(values_dbf2[key_dbf2]) == str(values_dbf1[key_dbf1]):
                            # print values_dbf2[key_dbf2], values_dbf1[key_dbf1]
                            break
                        else:
                            # ZQZH	A239716148	bef_XWH	20209	aft_XWH	3456
                            be_key_dbf1 = 'bef_' + table_stru[key_dbf2]
                            af_key_dbf2 = 'aft_' + table_stru[key_dbf2]
                            for i in PRIMARY_KEY:
                                diff_dict[table_stru[i]] = values_dbf1[i]
                            # diff_dict[table_stru[i]] = values_dbf1[i]
                            if values_dbf1[key_dbf1]:
                                if str(values_dbf1[key_dbf1]).lower() == 'nan':
                                    dbf1_value = u'NULL'
                                dbf1_value = values_dbf1[key_dbf1]
                            else:
                                dbf1_value = u'NULL'
                            if values_dbf2[key_dbf2]:
                                dbf2_value = values_dbf2[key_dbf2]
                            else:
                                dbf2_value = u'NULL'
                            diff_dict[be_key_dbf1] = dbf1_value
                            diff_dict[af_key_dbf2] = dbf2_value
                            break
            return diff_dict
        except Exception as e:
            print('进一步对比失败', e)

    def data_begin(self, df, df2, bxt, primary_key):

        print(df.head())
        print(df2.head())

        keys = list(df.keys())  # 拿到表结构

        primary_index = self.get_primary_key(primary_key, keys)  # 主键名列表，表结构字段，返回的是主键角标

        # max_circle_i = len(df)  # bef数据的最大长度

        bef_list = list(df.values)  # 导入进内存
        aft_list = list(df2.values)

        diff_list = []  # 存放差异json

        i = 0
        count = 0
        while i < len(bef_list):  # while进行删除操作
            if count % 1000 == 0:
                print('当前线程：{}，已经循环 -----> '.format(self.name), count)
            j = 0
            while j < len(aft_list):

                bef_data = list(bef_list[i])  # 升级前某行数据细节
                aft_data = list(aft_list[j])  # 升级后某行数据细节
                if self.judge_primary(primary_index, bef_data, aft_data):  # 判断主键是否一致，传递主键，表结构列表，需要比较的数据(i, j)

                    s = self.dbf2_opr(bef_data, aft_data, primary_index, bxt, keys)

                    if s:  # 存在差异
                        diff_list.append(s)  # json保存差异，传递4个数据，主键，需要比较的数据(i, j)，表结构列表，非比较字段

                    bef_list.pop(i)
                    aft_list.pop(j)
                    i -= 1
                    j -= 1
                    break

                j = j + 1
            i = i + 1
            count = count + 1

        print('dbf主键比对结束')
        print()
        return diff_list, bef_list, aft_list

    def gene_txt(self, context, flag=1):
        try:
            if flag == 1:
                file_name = self.DBF_FILE_NAME['BEF'].split('.')[0] + u'差异' + '.txt'
                with open(file_name, 'w+') as f:
                    f.write(context)
                return 1, u'success'
            else:
                file_name = self.DBF_FILE_NAME['BEF'].split('.')[0] + u'计时文件.txt'
                new_context = u'time is {}'.format(context)
                with open(file_name, 'w+') as f:
                    f.write(new_context)
                return 1, u'success'
        except Exception as e:
            # print u'txt写入出现异常 --> ', e
            return -1, u'fail'

    def write_dict_to_csv(self, data_json, bef_list, aft_list, fileName, table_name):
            """
            # 功能：将一字典写入到csv文件中
            # 输入：文件名称，数据字典
        [
            {
                "ZQZH": "A239716148",
                "be_BH": "NULL",
                "af_BH": "666"
            },
            {
                "ZQZH": "A190596220",
                "be_XWH": "20209",
                "af_XWH": "20409"
            },
            {
                "ZQZH": "A467554959",
                "be_BDRQ": "20180830",
                "af_BDRQ": "20135356",
                "be_BY": "NULL",
                "af_BY": "222"
            }
        ]
            """
            # print u'正在进行csv文件写入......'
            # print data_json
            try:  # 文件写入进行异常捕获操作
                with open(fileName, 'w', newline='') as csvFile:
                    csvWriter = csv.writer(csvFile, delimiter=',')
                    for item in data_json:
                        if item:
                            csv_list = []
                            for k, v in item.items():
                                k = str(k)
                                v = str(v)
                                csv_list.append(str(k))
                                if v.isnumeric() and v.startswith('0'):
                                    v = "\'" + v
                                csv_list.append(str(v))
                            # print u'csv_list -----------------------------> ', csv_list
                            csvWriter.writerow(csv_list)
                        else:
                            pass

                    for last_num in bef_list:
                        # code, exec_status = self.write_dict_to_csv(diff_all_json, bef_list, aft_list, file_name, self.TABLE_STRU)  # 将差异json写入csv文件
                        queue_list = []
                        for i in range(len(last_num)):
                            key = 'aft_' + table_name[i]
                            if key == '_NullFlags':
                                break
                            if str(last_num[i]).lower() == 'nan':
                                last_num[i] = u'NULL'
                            value = last_num[i]
                            queue_list.append(key)
                            queue_list.append(value)
                        csvWriter.writerow(queue_list)

                    for last_num in aft_list:
                        # code, exec_status = self.write_dict_to_csv(diff_all_json, bef_list, aft_list, file_name, self.TABLE_STRU)  # 将差异json写入csv文件
                        queue_list = []
                        for i in range(len(last_num)):
                            key = 'aft_' + table_name[i]
                            if key == '_NullFlags':
                                break
                            if str(last_num[i]).lower() == 'nan':
                                last_num[i] = u'NULL'
                            value = last_num[i]
                            queue_list.append(key)
                            queue_list.append(value)
                        csvWriter.writerow(queue_list)

                    csvFile.close()
                return 1, u'success'

            except Exception as e:
                print('csv写入出现异常 --> ', e)
                return -1, u'fail'

    def run(self):

        # self.DBF_FILE_NAME = DBF_FILE_NAME
        # self.NO_CHECK_PARAM = NO_CHECK_PARAM
        # self.primary_key = primary
        begin_time = datetime.datetime.now()

        print('已进入对比线程 ---------------------->')
        print()

        FILE_NAME = self.DBF_FILE_NAME['BEF']

        FILE_NAME_FAKE = self.DBF_FILE_NAME['AFT']

        bxt = self.NO_CHECK_PARAM

        primary_key = self.primary_key

        print('参数初始化已经结束------------------>')
        print()

        file_encoding = (chardet.detect(open(FILE_NAME, 'rb').read(10000)))['encoding']
        df = pd.read_csv(FILE_NAME, low_memory=False, encoding=file_encoding)
        print('升级前数据已进入内存----------------->')
        print()

        df.sort_values(by=primary_key)
        print('升级前数据已经排序------------------>')
        print()

        file_encoding_2 = (chardet.detect(open(FILE_NAME_FAKE, 'rb').read(10000)))['encoding']
        df2 = pd.read_csv(FILE_NAME_FAKE, low_memory=False, encoding=file_encoding_2)
        print('升级后数据已进入内存------------------>')
        print()

        df2.sort_values(by=primary_key)
        print('升级后数据已经排序------------------>')
        print()

        diff_all_json, bef_list, aft_list = self.data_begin(df, df2, bxt, primary_key)
        diff_txt_json = json.dumps(diff_all_json, ensure_ascii=False, indent=4)  # 格式化输出json字符串
        code, exec_status = self.gene_txt(diff_txt_json)  # 将差异json写入txt
        file_name = self.DBF_FILE_NAME['BEF'].split('.')[0] + u'差异' + '.csv'  # 设置差异csv文件名
        code, exec_status = self.write_dict_to_csv(diff_all_json, bef_list, aft_list, file_name, list(df.keys().values))
        # 将差异json写入csv文件

        # print(diff_all_json, bef_list, aft_list)

        # for i in diff_all_json:
        #     print('all_diff -->', i)
        #
        # for j in bef_list:
        #     print('bef_diff -->', j)
        #
        # for k in aft_list:
        #     print('aft_list -->', k)
        end_time = datetime.datetime.now()
        time_code, time_exec_status = self.gene_txt((end_time-begin_time).seconds, flag=2)  # 生成用时txt


if __name__ == '__main__':
    """
    DICT: 文件目录
    BEF: 升级前文件名
    AFT: 升级后文件名
    NO_CHECK_PARAM: 非对比的参数
    PRIMARY_KEY: 主键列
    """
    """
    if os.path.exists("settings.json"):
    _setting = json.loads(open("settings.json", "r").read())
    """
    try:
        print('正在打开配置文件...')
        f = open('csv_settings.json').read()
        DBF_FILE_JSON = json.loads(f)
        dbf_thread_list = []

        for i in DBF_FILE_JSON:
            DBF_FILE_NAME = DBF_FILE_JSON[i]['CSV_FILE_NAME']

            NO_CHECK_PARAM = DBF_FILE_JSON[i]['NO_CHECK_PARAM']

            primary_key = DBF_FILE_JSON[i]['PRIMARY_KEY']

            exec_func = BdfTask(DBF_FILE_NAME, NO_CHECK_PARAM, primary_key)

            dbf_thread_list.append(exec_func)

        for j in dbf_thread_list:
            j.start()

        for j in dbf_thread_list:
            j.join(0.01)

        print('主线程结束')

    except Exception as e:
        print('主线程抛出异常', e)

