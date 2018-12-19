# -*- coding:gbk -*-
import json

from collections import OrderedDict
import os

import datetime
from dbfread import DBF

import csv
import threading

import sys
reload(sys)
sys.setdefaultencoding('gbk')


class BdfTask(threading.Thread):
    def __init__(self, DBF_FILE_NAME, NO_CHECK_PARAM, PRIMARY_KEY):
        threading.Thread.__init__(self)
        self.DBF_FILE_NAME = DBF_FILE_NAME
        self.NO_CHECK_PARAM = NO_CHECK_PARAM
        self.PRIMARY_KEY = PRIMARY_KEY

    def read_dbf_file(self, file_name, file_dir):
        print u'正在读取dbf文件......  ', file_name
        try:
            _file = os.path.join(file_dir, file_name)
            table = DBF(_file, encoding='gbk', load=True)
            print u'dbf文件读取结束  ', file_name
            # table_list = list(table)  此处可以优化算法  -->  46行
            return table
        except Exception as e:
            print u'读取文件失败', e

    def dbf1_opr(self, dbf_1, dbf_2, PRIMARY_KEY, *bxt):  # 先进行主键比对，使用*bxt列表进行非比对字段控制
        try:
            diff_list = []
            print u' 正在进行dbf主键比对......'
            print
            print dbf_1
            for values_dbf1 in dbf_1:
                for values_dbf2 in dbf_2:
                    if values_dbf2[PRIMARY_KEY] == values_dbf1[PRIMARY_KEY]:
                        diff_list.append(self.dbf2_opr(values_dbf1, values_dbf2, PRIMARY_KEY, bxt))
                        # dbf_2.pop(values_dbf2)  # 此处可以优化算法  -->  31行
                        print len(dbf_2)
                        break
            print u' dbf主键比对结束'
            print
            return diff_list
        except Exception as e:
            print u'主键对比失败', e

    def dbf2_opr(self, values_dbf1, values_dbf2, PRIMARY_KEY, bxt):
        # 将具体的列进行进一步比对
        try:
            diff_dict = OrderedDict()
            print u'     正在进行进一步行列比对......', values_dbf1.keys()[0:5], values_dbf2.keys()[0:5]
            for key_dbf1 in values_dbf1:
                for key_dbf2 in values_dbf2:
                    if bxt:
                        if key_dbf2 in bxt[0] or key_dbf1 in bxt[0]:
                            continue
                    else:
                        pass
                    if key_dbf2 == key_dbf1:
                        if values_dbf2[key_dbf2] == values_dbf1[key_dbf1]:
                            continue
                        else:
                            be_key_dbf1 = 'bef_' + key_dbf1
                            af_key_dbf2 = 'aft_' + key_dbf2
                            diff_dict[PRIMARY_KEY] = values_dbf2[PRIMARY_KEY]
                            if values_dbf1[key_dbf1]:
                                dbf1_value = values_dbf1[key_dbf1]
                            else:
                                dbf1_value = u'NULL'
                            if values_dbf2[key_dbf2]:
                                dbf2_value = values_dbf2[key_dbf2]
                            else:
                                dbf2_value = u'NULL'
                            diff_dict[be_key_dbf1] = dbf1_value
                            diff_dict[af_key_dbf2] = dbf2_value
            print u'     进一步行列比对结束'
            print
            return diff_dict
        except Exception as e:
            print u'进一步对比失败', e

    def write_dict_to_csv(self, data_json, fileName):
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
        print u'正在进行csv文件写入......'
        # print data_json
        try:  # 文件写入进行异常捕获操作
            with open(fileName, 'wb') as csvFile:
                csvWriter = csv.writer(csvFile, delimiter=',')
                for item in data_json:
                    if item:
                        csv_list = []
                        for k, v in item.iteritems():
                            csv_list.append(str(k))
                            csv_list.append(str(v))
                        csvWriter.writerow(csv_list)
                    else:
                        pass
                csvFile.close()
            return 1, u'success'

        except Exception as e:
            print u'csv写入出现异常 --> ', e
            return -1, u'fail'

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
            print u'txt写入出现异常 --> ', e
            return -1, u'fail'

    def dbf_to_csv(self):
        """
        整体转换方法  -->
        csv_title_list = []
        with open('hehe.csv', 'wb') as csvFile:
            csvWriter = csv.writer(csvFile)
            for field in table.fields:
                csv_title_list.append(field.name)
            csvWriter.writerow(csv_title_list)
            for field in table:
                csv_content_list = []
                for i in field:
                    csv_content_list.append(field[i])
                csvWriter.writerow(csv_content_list)
                del csv_content_list
        :return:
        """
        dbf_1 = self.read_dbf_file(self.DBF_FILE_NAME['BEF'], self.DBF_FILE_NAME['DICT'])
        dbf_2 = self.read_dbf_file(self.DBF_FILE_NAME['AFT'], self.DBF_FILE_NAME['DICT'])

        file_bef = self.DBF_FILE_NAME['BEF'].split('.')[0] + '.csv'
        file_aft = self.DBF_FILE_NAME['AFT'].split('.')[0] + '.csv'

        self.write_csv(file_bef, dbf_1)
        self.write_csv(file_aft, dbf_2)
        print u'csv文件生成成功', file_bef, file_aft

    def write_csv(self, file_name, dbf_data):
        try:
            csv_title_list = []
            with open(file_name, 'wb') as csvFile:
                csvWriter = csv.writer(csvFile)
                for field in dbf_data.fields:
                    csv_title_list.append(str(field.name))
                csvWriter.writerow(csv_title_list)
                for field in dbf_data:
                    csv_content_list = []
                    for i in field:
                        csv_content_list.append(str(field[i]))
                    csvWriter.writerow(csv_content_list)
                    del csv_content_list
                csvFile.close()
        except Exception as e:
            print u'dbf转换csv出错', e

    def run(self):

        begin_time = datetime.datetime.now()

        self.dbf_to_csv()  # 先进行dbf --> csv写入

        dbf_1 = self.read_dbf_file(self.DBF_FILE_NAME['BEF'], self.DBF_FILE_NAME['DICT'])
        dbf_2 = self.read_dbf_file(self.DBF_FILE_NAME['AFT'], self.DBF_FILE_NAME['DICT'])

        diff_all_json = self.dbf1_opr(dbf_1, dbf_2, self.PRIMARY_KEY, self.NO_CHECK_PARAM)  # 拿到比对后的json
        diff_txt_json = json.dumps(diff_all_json, ensure_ascii=False, indent=4, encoding='utf-8')  # 格式化输出json字符串
        code, exec_status = self.gene_txt(diff_txt_json)  # 将差异json写入txt
        print code, exec_status
        file_name = self.DBF_FILE_NAME['BEF'].split('.')[0] + u'差异' + '.csv'
        code, exec_status = self.write_dict_to_csv(diff_all_json, file_name)  # 将差异json写入csv文件
        print code, exec_status

        end_time = datetime.datetime.now()

        print u'共用时 --> ', (end_time-begin_time).seconds

        time_code, time_exec_status = self.gene_txt((end_time-begin_time).seconds, flag=2)  # 生成用时txt

        print time_code, time_exec_status

        print u'当前线程: ', threading.current_thread


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
        f = open('dbf_settings.json').read()
        DBF_FILE_JSON = json.loads(f)
        dbf_thread_list = []

        for i in DBF_FILE_JSON:
            DBF_FILE_NAME = DBF_FILE_JSON[i]['DBF_FILE_NAME']

            NO_CHECK_PARAM = DBF_FILE_JSON[i]['NO_CHECK_PARAM']

            PRIMARY_KEY = DBF_FILE_JSON[i]['PRIMARY_KEY']

            exec_func = BdfTask(DBF_FILE_NAME, NO_CHECK_PARAM, PRIMARY_KEY)

            dbf_thread_list.append(exec_func)

        for j in dbf_thread_list:

            j.start()

        for j in dbf_thread_list:
            j.join(0.01)

        print u'主线程结束'

    except Exception as e:
        print u'主线程抛出异常', e

