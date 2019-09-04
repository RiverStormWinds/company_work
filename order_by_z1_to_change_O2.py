# -*- coding: utf-8 -*-
'''
测试用例的迁移
'''
import collections
import MySQLdb
import pyodbc
import sys

reload(sys)
sys.setdefaultencoding('GBK')
SX_CASE_ALL_DATA = []
SX_CASES_DETAIL_ALL_DATA = []
SX_CASES_DETAIL_PARAM_DATA_ALL_DATA = []
SX_CASES_PARAM_DATA_ALL_DATA = []

CASES_ID = 0
CASES_DETAIL_ID = 0
PARAM_DATA_ID = 0
CASES_PARAM_DATA_ID = 0


# # 数据库连接 #记录tbl_cases分化的四张表数据
# def get_Dbconnection_test():
#     return MySQLdb.connect(host='localhost', port=3306, user='root',
#                            passwd='123456', db='jzauto_bak', charset='GBK')


# 获取tbl_cases信息
def getDbconnection_zhlc():
    return MySQLdb.connect(host='localhost', port=3306, user='root',
                           passwd='123456', db='jzauto', charset='GBK')



def getConnected():
    try:
        type = 'DB2'
        conn = None
        if type == 'DB2':
            dbstring = "driver={IBM DB2 ODBC DRIVER};database=AUTOTEST;hostname=10.181.101.151;port=50000;protocol=TCPIP;uid=autotest;pwd=1GaTjest;"
            # dbstring = "driver={IBM DB2 ODBC DRIVER};database=CSZDH;hostname=10.189.96.6;port=50000;protocol=TCPIP;uid=TIMESVC;pwd=timesvc;"
            # dbstring = "driver={IBM DB2 ODBC DRIVER};database=TIMESVC;hostname=10.187.96.46;port=50000;protocol=TCPIP;uid=timesvc;pwd=csgl@2017;"
            # dbstring = "driver={IBM DB2 ODBC DRIVER};database=ZDH;hostname=10.189.111.9;port=50000;protocol=TCPIP;uid=AUTOTEST;pwd=gtja.8888;"
            conn = pyodbc.connect(dbstring)
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        print e

def get_row_id(col, table_name):
    # 获取旧版本的综合理财自动化测试系统的测试用库获tbl_cases
    conn, cursor = getConnected()
    try:
        sql = "select max(%s) FROM %s" % (col, table_name)
        cursor.execute(sql)
        result = cursor.fetchall()
        max_id = 0
        if result:
            max_id = result[0][0]
        return max_id
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


def get_tbl_cases_data():
    # 获取旧版本的综合理财自动化测试系统的测试用库获tbl_cases
    conn2 = getDbconnection_zhlc()
    cursor = conn2.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    try:
        sql = u"select * from tbl_cases where disableflag = 0  order by itempos, groupname, case_index"
        cursor.execute(sql)
        result = cursor.fetchall()
        # 按照组别信息分组
        result, groupname_new_old = group_by_tbl_cases_data(list(result))
        return result, groupname_new_old
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn2.close()

def group_by_tbl_cases_data(data):
    '''根据groupname的值分组'''
    result = collections.OrderedDict()
    keys = []
    groupname_new_old = {}
    for i in data:
        child = []
        groupname = ""
        if str(i["groupname"]):
            groupname = str(i["groupname"]) + "itempos" +  str(i["itempos"]) + "=" + str(i['servertype'])
        else:
            groupname = '未分类'
        if groupname not in keys:
            if not i["groupname"]:
                groupname_new_old[str(groupname)] = u'未分类'
            else:
                groupname_new_old[str(groupname)] = i["groupname"]
            keys.append(groupname)
            child.append(i)
            result[groupname] = child
        else:
            child_data = result[groupname]
            child_data.append(i)
            result[groupname] = child_data

    return result, groupname_new_old
# def group_by_tbl_cases_data(data):
#     '''根据groupname的值分组'''
#     result = collections.OrderedDict()
#     keys = []
#     for i in data:
#         child = []
#         if i["groupname"] not in keys:
#             keys.append(i["groupname"])
#             child.append(i)
#             result[i["groupname"]] = child
#         else:
#             child_data = result[i["groupname"]]
#             child_data.append(i)
#             result[i["groupname"]] = child_data
#
#     return result


def sava_sx_cases_info():
    '''先初始化各表的数先，再保存各表的数据'''
    try:
        tree_directory_id_info, data_TREE_DIRECTORY_ID = order_by_itempos_to_get_tree_directory_id()
        # del_table()
        tree_directory_id_keys = tree_directory_id_info.keys()
        case_data, groupname_new_old = get_tbl_cases_data()
        # id_data = query_id_order_by_funcid()
        count = 0
        for index, i in enumerate(case_data):
            print index
            #i为组名
            groupname = groupname_new_old[str(i)]
            # groupname = i.split("=")[0].decode('gbk')
            if case_data[i]:
                # 每组对应的数据
                CASES_CODE = "JZJY000000"
                count += 1
                CASES_CODE = CASES_CODE + str(count)
                itempos = case_data[i][0]["itempos"]
                servertype = case_data[i][0]["servertype"]
                TIMESLICE = case_data[i][0]["timeslice"]
                itempos_type = str(itempos) + '=' + str(servertype)
                TREE_DIRECTORY_ID = 0
                if itempos_type in tree_directory_id_keys:
                    TREE_DIRECTORY_ID = tree_directory_id_info[str(itempos_type)]
                    if not TREE_DIRECTORY_ID:
                        TREE_DIRECTORY_ID = 0
                # for tree_directory_ids in tree_directory_id_info:
                #     if str(servertype) == tree_directory_ids['type'] and str(itempos) == tree_directory_ids['itempos']:
                #         TREE_DIRECTORY_ID = tree_directory_ids['TREE_DIRECTORY_ID']
                #         break
                args = (CASES_CODE, groupname, "CASE_LEVEL_HIGH", '0', "142891263425277DZ1", int(TREE_DIRECTORY_ID), TIMESLICE)
                global SX_CASE_ALL_DATA
                SX_CASE_ALL_DATA.append(args)
                global CASES_ID
                CASES_ID += 1
                sava_sx_cases_detail(CASES_ID, case_data[i], TREE_DIRECTORY_ID, data_TREE_DIRECTORY_ID)
    except Exception as e:
        print e


def order_by_itempos_to_get_tree_directory_id():
    '''根据itempos查询tree_directory_id'''
    conn, cursor = getConnected()
    try:
        data = {}
        data_TREE_DIRECTORY_ID = {}
        key_values = {"CTL0": "z", "PT9": "p", "VIP": "b", "VIP2": "v", "QJ90": "q"}
        sql = "select  DIRECTORY_NAME, itempos, TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY " \
              "WHERE SYSTEM_ID='142891263425277DZ1' ORDER BY TREE_DIRECTORY_ID"
        cursor.execute(sql)
        type = ""
        for row in cursor.fetchall():
            DIRECTORY_NAME, itempos, TREE_DIRECTORY_ID = row
            DIRECTORY_NAME = DIRECTORY_NAME.decode('gbk', 'ignore').encode('utf-8')
            if str(DIRECTORY_NAME) == "CTL0":
                type = 'z'
            elif str(DIRECTORY_NAME) == "PT9":
                type = 'p'
            elif str(DIRECTORY_NAME) == "VIP":
                type = 'b'
            elif str(DIRECTORY_NAME) == "VIP2":
                type = 'v'
            elif str(DIRECTORY_NAME) == "QJ90":
                type = 'q'
            else:
                pass
            if type:
                itempos_type = str(itempos) + '=' + str(type)
                # one_record['type'] = str(type)
                # one_record['itempos'] = str(itempos)
                data[str(itempos_type)] = int(TREE_DIRECTORY_ID)
                data_TREE_DIRECTORY_ID[str(TREE_DIRECTORY_ID)] = itempos_type
                # data.append(one_record)
        return data, data_TREE_DIRECTORY_ID
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


# def order_by_itempos_to_get_tree_directory_id():
#     '''根据itempos查询tree_directory_id'''
#     type = 'DB2'
#     conn = None
#     if type == 'DB2':
#         dbstring = "driver={IBM DB2 ODBC DRIVER};database=TIMESVC;hostname=10.187.96.46;port=50000;protocol=TCPIP;uid=timesvc;pwd=csgl@2017;"
#         conn = pyodbc.connect(dbstring)
#     cursor = conn.cursor()
#     try:
#         data = {}
#         sql = "select itempos, TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY WHERE SYSTEM_ID='142891263425277DZ1'"
#         cursor.execute(sql)
#         for row in cursor.fetchall():
#             itempos, TREE_DIRECTORY_ID = row
#             child_data = {str(itempos): TREE_DIRECTORY_ID}
#             data.update(child_data)
#         return data
#     except Exception as e:
#         print e
#     finally:
#         cursor.close()
#         conn.close()


def sava_sx_cases_detail_param_data(CASES_DETAIL_ID, data):
    # type = 'DB2'
    # conn = None
    # if type == 'DB2':
    #     dbstring = "driver={IBM DB2 ODBC DRIVER};database=TIMESVC;hostname=10.187.96.46;port=50000;protocol=TCPIP;uid=timesvc;pwd=csgl@2017;"
    #     conn = pyodbc.connect(dbstring)
    # cursor = conn.cursor()
    try:
        PARAM_DATA = ""
        if data["cmdstring"]:
            cmdstring, INPUT_STRING = split_test(data["cmdstring"])
            cmdstring = cmdstring.replace("#", "&bfb&").replace("=", "&bft&")
            PARAM_DATA = cmdstring.replace(",", "#").replace(":", "=")
            PARAM_DATA = PARAM_DATA.replace("'", "''")
        args = (CASES_DETAIL_ID, PARAM_DATA, data["expect_ret"],
                data["account_group_id"])
        # sql = "insert into SX_CASES_DETAIL_PARAM_DATA(CASES_DETAIL_ID, PARAM_DATA, EXPECTED_VALUE, ACCOUNT_GROUP_ID) " \
        #       "VALUES(%s, '%s', '%s', %s)" % args

        global SX_CASES_DETAIL_PARAM_DATA_ALL_DATA
        SX_CASES_DETAIL_PARAM_DATA_ALL_DATA.append(args)
        global PARAM_DATA_ID
        PARAM_DATA_ID += 1
        # cursor.execute(sql)
        # conn.commit()
        # PARAM_DATA_ID = conn.insert_id()
        # PARAM_DATA_ID = get_row_id('PARAM_DATA_ID', 'SX_CASES_DETAIL_PARAM_DATA')
        # sql = """update SX_CASES_DETAIL_PARAM_DATA set PARAM_DATA=replace(PARAM_DATA, '!@#$^&*', '''')
        #             WHERE PARAM_DATA_ID=%s""" % PARAM_DATA_ID
        # cursor.execute(sql)
        # conn.commit()
        return PARAM_DATA_ID
    except Exception as e:
        print e
        # finally:
        #     cursor.close()
        #     conn.close()


def query_id_order_by_funcid(funcid, funcname):
    '''所属funcid 在 sx_interface_info 的ID===> INTERFACE_ID'''
    conn, cursor = getConnected()
    try:
        funcname = "%" + funcname + "%"
        sql = "select ID from  SX_INTERFACE_INFO where SYSTEM_ID= '142891263425277DZ1' and FUNCID='%s' and INTERFACE_NAME like '%s'" % (funcid, funcname)
        cursor.execute(sql)
        result = cursor.fetchall()
        ID = ""
        if result:
            ID = result[0][0]
        if ID == "":
            sql = "select ID from  SX_INTERFACE_INFO where SYSTEM_ID= '142891263425277DZ1' and FUNCID='%s'" % (funcid)
            cursor.execute(sql)
            result = cursor.fetchall()
            if result:
                ID = result[0][0]
        return ID
    except Exception as e:
        print e
    finally:
        cursor.close()
        conn.close()


# def query_id_order_by_funcid():
#     # 为funcid对应的sx_interface_info的id
#     type = 'DB2'
#     conn = None
#     if type == 'DB2':
#         dbstring = "driver={IBM DB2 ODBC DRIVER};database=TIMESVC;hostname=10.187.96.46;port=50000;protocol=TCPIP;uid=timesvc;pwd=csgl@2017;"
#         conn = pyodbc.connect(dbstring)
#     cursor = conn.cursor()
#     try:
#         data = {}
#         sql = "select ID, FUNCID from SX_INTERFACE_INFO"
#         cursor.execute(sql)
#         for row in cursor.fetchall():
#             ID, FUNCID = row
#             if FUNCID:
#                 sql = "update SX_CASES_DETAIL set PARAM_ID=%s where FUNCID='%s'"%(ID, str(FUNCID))
#                 cursor.execute(sql)
#         return data
#     except Exception as e:
#         print e
#     finally:
#         conn.commit()
#         cursor.close()
#         conn.close()


def sava_sx_cases_detail(case_id, data, TREE_DIRECTORY_ID, data_TREE_DIRECTORY_ID):
    # type = 'DB2'
    # conn = None
    # if type == 'DB2':
    #     dbstring = "driver={IBM DB2 ODBC DRIVER};database=TIMESVC;hostname=10.187.96.46;port=50000;protocol=TCPIP;uid=timesvc;pwd=csgl@2017;"
    #     conn = pyodbc.connect(dbstring)
    # cursor = conn.cursor()
    try:
        PARAM_DATA_IDs = []
        for i in data:
            # flag = True
            # id = ""
            # keys = id_data.keys()
            # if not i["funcid"]:
            #     flag = False
            # elif i["funcid"] in keys:
            #     id = id_data[str(i["funcid"])]
            # else:
            #     pass
            # if not id:
            #     flag = False
            # print id
            # funcid对应的sx_interface_info的id, id对应PARAM_ID
            # INPUT_STRING = ""
            # if i["cmdstring"]:
            #     INPUT_STRING = "".join([str(j.split(":")[0]) + "=#" for j in i["cmdstring"].split(",")])
            # if INPUT_STRING:
            #     INPUT_STRING = INPUT_STRING.replace("'", "''")

            INPUT_STRING = ""
            if i["cmdstring"]:
                if i["cmdstring"].endswith(","):
                    i["cmdstring"] = i["cmdstring"][:-1]
                p, INPUT_STRING = split_test(i["cmdstring"])
                # INPUT_STRING = "".join([str(j.split(":")[0]) + "=#" for j in i["cmdstring"].split(",")])
            if INPUT_STRING:
                INPUT_STRING = INPUT_STRING.replace("'", "''")

            sql = ""
            args = ()
            pre_action = ""
            pro_action = ""
            if i["pre_action"]:
                # pre_action = str(i["pre_action"]).replace("'", "''")
                pre_action = i["pre_action"].replace("'", "''")
            if i["pro_action"]:
                pro_action = i["pro_action"].replace("'", "''")
            if pre_action == 'None':
                pre_action = ""
            if pro_action == 'None':
                pro_action = ""
            # id = ""
            # id = orderby_TREE_DIRECTORY_ID_to_get_funcid(i['servertype'], i['itempos'], TREE_DIRECTORY_ID, i['funcid'], data_TREE_DIRECTORY_ID)
            # if id:
            #     print u"接口ID：%s" % id
            #     # if not id:
            #     #     id = 0
            #     args = (case_id, i['casename'], i["case_index"], "SPLX_KCBP",
            #             INPUT_STRING,
            #             pre_action,
            #             pro_action, id, i['funcid'],
            #             i['disableflag'], i['servertype'])
            #     # sql = "insert into SX_CASES_DETAIL(CASES_ID, STEP_NAME, STEP, ADAPTER_TYPE, " \
            #     #       "INPUT_STRING, PRE_ACTION, PRO_ACTION, PARAM_ID, FUNCID, IS_DELETE, SERVER_TYPE)  " \
            #     #       "VALUES(%s, '%s', %s, '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s')" % args
            # else:
            args = (case_id, i['casename'], int(i["case_index"]), "SPLX_KCBP", INPUT_STRING, pre_action,
                    pro_action, 0, i['funcid'], "0", i['servertype'])
            # sql = "insert into SX_CASES_DETAIL(CASES_ID, STEP_NAME, STEP, ADAPTER_TYPE, " \
            #       "INPUT_STRING, PRE_ACTION, PRO_ACTION, FUNCID, IS_DELETE, SERVER_TYPE)" \
            #       " VALUES(%s,  '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % args
            global SX_CASES_DETAIL_ALL_DATA
            SX_CASES_DETAIL_ALL_DATA.append(args)
            # cursor.execute(sql)
            # conn.commit()
            # CASES_DETAIL_ID = get_row_id('CASES_DETAIL_ID', 'SX_CASES_DETAIL')
            global CASES_DETAIL_ID
            CASES_DETAIL_ID += 1
            PARAM_DATA_ID = sava_sx_cases_detail_param_data(CASES_DETAIL_ID, i)
            PARAM_DATA_IDs.append(str(PARAM_DATA_ID))
        sava_sx_cases_param_data_info(case_id, PARAM_DATA_IDs)
    except Exception as e:
        print e
        # finally:
        #     # conn.commit()
        #     cursor.close()
        #     conn.close()


def sava_sx_cases_param_data_info(case_id, PARAM_DATA_IDs):
    # type = 'DB2'
    # conn = None
    # if type == 'DB2':
    #     dbstring = "driver={IBM DB2 ODBC DRIVER};database=TIMESVC;hostname=10.187.96.46;port=50000;protocol=TCPIP;uid=timesvc;pwd=csgl@2017;"
    #     conn = pyodbc.connect(dbstring)
    # cursor = conn.cursor()
    try:
        PARAM_DATA_ID_STR = ""
        if PARAM_DATA_IDs:
            PARAM_DATA_ID_STR = ",".join(PARAM_DATA_IDs)
        args = (case_id, u"基础数据", "CASE_LEVEL_MIDDLE", PARAM_DATA_ID_STR)
        # sql = "insert into SX_CASES_PARAM_DATA(CASES_ID, PARAM_NAME, PRI, PARAM_DATA_ID_STR) VALUES(%s, '%s', '%s', '%s')" % args
        # cursor.execute(sql)
        global SX_CASES_PARAM_DATA_ALL_DATA
        SX_CASES_PARAM_DATA_ALL_DATA.append(args)

        # global CASES_PARAM_DATA_ID
        # CASES_PARAM_DATA_ID += 1

    except Exception as e:
        print e
        # finally:
        #     conn.commit()
        #     cursor.close()
        #     conn.close()


def del_table():
    '''清除数据'''
    conn, cursor = getConnected()
    case_id, CASES_DETAIL_IDs = get_case_id()
    if not case_id:
        return
    table_name = ["SX_CASES_PARAM_DATA", "SX_CASES_DETAIL", "SX_CASES",
                  "SX_CASES_DETAIL_PARAM_DATA"]
    col_name = ['CASES_DETAIL_ID', 'CASES_ID']
    try:
        for i in table_name:
            col = ""
            ids = []
            if i == "SX_CASES_DETAIL_PARAM_DATA":
                if not CASES_DETAIL_IDs:
                    continue
                col = col_name[0]
                ids = ",".join(CASES_DETAIL_IDs)
            else:
                col = col_name[1]
                if not case_id:
                    continue
                ids = ",".join(case_id)
            ids_list = ids.split(",")
            num = int(len(ids_list) / 2)
            ids1 = ids_list[:num]
            ids1 = ",".join(ids1)
            ids2 = ids_list[num:]
            ids2 = ",".join(ids2)
            sql = "delete from %s where %s in (%s)" % (i, col, ids1)
            cursor.execute(sql)
            sql = "delete from %s where %s in (%s)" % (i, col, ids2)
            cursor.execute(sql)

    except Exception as e:
        print e
    finally:
        conn.commit()
        cursor.close()
        conn.close()


def get_case_id():
    conn, cursor = getConnected()
    sql = "select CASES_ID, CASES_CODE from SX_CASES where SYSTEM_ID='142891263425277DZ1'"
    cursor.execute(sql)
    case_id = []
    for row in cursor.fetchall():
        CASES_ID, CASES_CODE = row
        case_id.append(str(CASES_ID))
    cursor.close()
    conn.close()
    CASES_DETAIL_IDs = []
    if case_id:
        CASES_DETAIL_IDs = get_case_detail_id(case_id)
    return case_id, CASES_DETAIL_IDs


def get_case_detail_id(case_id):
    conn, cursor = getConnected()
    case_id = ",".join(case_id)
    sql = "select CASES_DETAIL_ID, CASES_ID from SX_CASES_DETAIL where CASES_ID in (" + case_id + ")"
    cursor.execute(sql)
    CASES_DETAIL_IDs = []
    for row in cursor.fetchall():
        CASES_DETAIL_ID, CASES_ID = row
        CASES_DETAIL_IDs.append(str(CASES_DETAIL_ID))
    cursor.close()
    conn.close()
    return CASES_DETAIL_IDs


# def TEST():
#     type = 'DB2'
#     conn = None
#     if type == 'DB2':
#         dbstring = "driver={IBM DB2 ODBC DRIVER};database=CSZDH;hostname=10.189.96.6;port=50000;protocol=TCPIP;uid=TIMESVC;pwd=timesvc;"
#         conn = pyodbc.connect(dbstring)
#     cursor = conn.cursor()
#     a = "tertertsertert'dfdfgsdsfg"
#     a = a.replace("'", "''")
#     sql = "INSERT INTO SX_CASES(CASES_CODE, CASE_NAME) VALUES('TEST', '%s')"%a
#     cursor.execute(sql)
#     conn.commit()
#     cursor.close()
#     conn.close()
# ALTER TABLE SX_CASES ALTER COLUMN CASES_ID RESTART WITH 1671
# select max(CASES_ID) from SX_CASES
#
# ALTER TABLE SX_CASES_DETAIL ALTER COLUMN CASES_DETAIL_ID RESTART WITH 6898
# select max(CASES_DETAIL_ID) from  SX_CASES_DETAIL
#
# ALTER TABLE SX_CASES_DETAIL_PARAM_DATA ALTER COLUMN PARAM_DATA_ID RESTART WITH 54532
#
# select max(PARAM_DATA_ID) from  SX_CASES_DETAIL_PARAM_DATA
#
# ALTER TABLE SX_CASES_PARAM_DATA ALTER COLUMN CASES_PARAM_DATA_ID RESTART WITH 1855
#
# select max(CASES_PARAM_DATA_ID) from SX_CASES_PARAM_DATA

def insert_data():
    insert_data_sx_case_all_data()
    insert_data_sx_cases_detail_all_data()
    insert_data_sx_cases_param_data_all_data()
    insert_data_sx_cases_detail_param_data_all_data()


def insert_data_sx_case_all_data():
    conn, cursor = getConnected()
    try:
        global SX_CASE_ALL_DATA
        print "start>>>>>>>>>>>>>>>>>>>> SX_CASE_ALL_DATA"
        args = []
        for i in SX_CASE_ALL_DATA:
            args1 = i[0]
            if isinstance(args1, unicode):
                args1 = args1.encode('unicode-escape').decode('string-escape')
            args2 = i[1]
            if isinstance(args2, unicode):
                args2 = args2.encode('unicode-escape').decode('string-escape')
            args2 = args2.replace("'", "''")
            args3 = i[2]
            if isinstance(args3, unicode):
                args3 = args3.encode('unicode-escape').decode('string-escape')
            args4 = i[3]
            if isinstance(args4, unicode):
                args4 = args4.encode('unicode-escape').decode('string-escape')
            args5 = i[4]
            if isinstance(args5, unicode):
                args5 = args5.encode('unicode-escape').decode('string-escape')
            args6 = i[5]
            if isinstance(args6, unicode):
                args6 = args6.encode('unicode-escape').decode('string-escape')
            args7 = i[6]
            if isinstance(args7, unicode):
                args7 = args7.encode('unicode-escape').decode('string-escape')
            data = "('%s', '%s', '%s', '%s', '%s', %s, %s)" % (args1, args2, args3, args4, args5, args6, args7)
            args.append(data.decode('unicode-escape'))
        COUNT = 0
        args = [args[i:i + 20] for i in range(0, len(args), 20)]
        for SX_CASES_args in args:
            COUNT += 1
            print COUNT
            sql = "insert into SX_CASES(CASES_CODE, CASE_NAME, CASE_LEVEL, IS_DELETE, SYSTEM_ID," \
                  " TREE_DIRECTORY_ID, TIMESLICE) values %s" % (','.join(SX_CASES_args))
            cursor.execute(sql)
            conn.commit()
        print "SX_CASES"
    except Exception as e:
        print e
    finally:
        conn.commit()
        cursor.close()
        conn.close()

def insert_data_sx_cases_param_data_all_data():
    conn, cursor = getConnected()
    try:
        global SX_CASES_PARAM_DATA_ALL_DATA
        print "start>>>>>>>>>>>>>>>>>>>> SX_CASES_PARAM_DATA_ALL_DATA"
        args = []
        COUNT = 0
        for i in SX_CASES_PARAM_DATA_ALL_DATA:
            args1 = i[0]
            if isinstance(args1, unicode):
                args1 = args1.encode('unicode-escape').decode('string-escape')
            args2 = i[1]
            if isinstance(args2, unicode):
                args2 = args2.encode('unicode-escape').decode('string-escape')
            args3 = i[2]
            if isinstance(args3, unicode):
                args3 = args3.encode('unicode-escape').decode('string-escape')
            args4 = i[3]
            if isinstance(args4, unicode):
                args4 = args4.encode('unicode-escape').decode('string-escape')
            data = "( %s, '%s', '%s', '%s')" % (args1, args2, args3, args4)
            args.append(data.decode('unicode-escape'))

        args = [args[i:i + 20] for i in range(0, len(args), 20)]
        for SX_CASES_PARAM_DATA_args in args:
            COUNT += 1
            print COUNT
            sql = "insert into SX_CASES_PARAM_DATA(CASES_ID, PARAM_NAME, PRI, PARAM_DATA_ID_STR) values %s" % (','.join(SX_CASES_PARAM_DATA_args))
            cursor.execute(sql)
            conn.commit()
        print "SX_CASES_PARAM_DATA"
    except Exception as e:
        print e
    finally:
        conn.commit()
        cursor.close()
        conn.close()

def insert_data_sx_cases_detail_all_data():
    conn, cursor = getConnected()
    try:
        global SX_CASES_DETAIL_ALL_DATA
        print "start>>>>>>>>>>>>>>>>>>>>SX_CASES_DETAIL_ALL_DATA"
        args = []
        for i in SX_CASES_DETAIL_ALL_DATA:
            args1 = i[0]
            if isinstance(args1, unicode):
                args1 = args1.encode('unicode-escape').decode('string-escape')
            args2 = i[1]
            if isinstance(args2, unicode):
                args2 = args2.encode('unicode-escape').decode('string-escape')
            args3 = i[2]
            if isinstance(args3, unicode):
                args3 = args3.encode('unicode-escape').decode('string-escape')
            args4 = i[3]
            if isinstance(args4, unicode):
                args4 = args4.encode('unicode-escape').decode('string-escape')
            args5 = i[4]
            if isinstance(args5, unicode):
                args5 = args5.encode('unicode-escape').decode('string-escape')


            args6 = i[5]
            if isinstance(args6, unicode):
                args6 = args6.encode('unicode-escape').decode('string-escape')
            # args6 = args6.encode('utf-8')

            args7 = i[6]
            if isinstance(args7, unicode):
                args7 = args7.encode('unicode-escape').decode('string-escape')
            # args7 = args7.encode('utf-8')


            args8 = i[7]
            if isinstance(args8, unicode):
                args8 = args8.encode('unicode-escape').decode('string-escape')
            args9 = i[8]
            if isinstance(args9, unicode):
                args9 = args9.encode('unicode-escape').decode('string-escape')
            args10 = i[9]
            if isinstance(args10, unicode):
                args10 = args10.encode('unicode-escape').decode('string-escape')
            args11 = i[10]
            if isinstance(args11, unicode):
                args11 = args11.encode('unicode-escape').decode('string-escape')

            data = "(%s, '%s', %s, '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s')" % (
            args1, args2, args3, args4, args5, args6, args7, args8, args9, args10, args11)
            args.append(data.decode('unicode-escape'))
        COUNT = 0
        args = [args[i:i + 100] for i in range(0, len(args), 100)]
        for SX_CASES_DETAIL_args in args:
            COUNT += 1
            print COUNT
            sql = "insert into SX_CASES_DETAIL(CASES_ID, STEP_NAME, STEP, ADAPTER_TYPE, " \
                  "INPUT_STRING, PRE_ACTION, PRO_ACTION, PARAM_ID, FUNCID, IS_DELETE, SERVER_TYPE)  values %s" % (','.join(SX_CASES_DETAIL_args))
            cursor.execute(sql)
            conn.commit()
        print "SX_CASES_DETAIL"
    except Exception as e:
        print e
    finally:
        conn.commit()
        cursor.close()
        conn.close()

def insert_data_sx_cases_detail_param_data_all_data():
    conn, cursor = getConnected()
    try:
        global SX_CASES_DETAIL_PARAM_DATA_ALL_DATA
        print "start>>>>>>>>>>>>>>>>>>>> SX_CASES_DETAIL_PARAM_DATA_ALL_DATA"
        args = []
        for i in SX_CASES_DETAIL_PARAM_DATA_ALL_DATA:
            args1 = i[0]
            if isinstance(args1, unicode):
                args1 = args1.encode('unicode-escape').decode('string-escape')
            args2 = i[1]
            if isinstance(args2, unicode):
                args2 = args2.replace("=\\",":\\")
                args2 = r"%s"%args2
                args2 = args2.encode('unicode-escape').decode('string-escape')
            args3 = i[2]
            if isinstance(args3, unicode):
                args3 = args3.encode('unicode-escape').decode('string-escape')
            args4 = i[3]
            if isinstance(args4, unicode):
                args4 = args4.encode('unicode-escape').decode('string-escape')
            data = "(%s, '%s', '%s', %s)" % (args1, args2, args3, args4)
            args.append(data.decode('unicode-escape'))
        COUNT = 0
        args = [args[i:i + 20] for i in range(0, len(args), 20)]
        for SX_CASES_DETAIL_PARAM_DATA_args in args:
            COUNT += 1
            print COUNT
            sql = "insert into SX_CASES_DETAIL_PARAM_DATA(CASES_DETAIL_ID, PARAM_DATA, EXPECTED_VALUE, ACCOUNT_GROUP_ID) values %s" % (','.join(SX_CASES_DETAIL_PARAM_DATA_args))
            cursor.execute(sql)
            conn.commit()
        print "SX_CASES_DETAIL_PARAM_DATA"
    except Exception as e:
        print e
    finally:
        conn.commit()
        cursor.close()
        conn.close()
def update_param_id():
    query_id_order_by_funcid()

# def update_sx_func_tree_directory_is_0():
#     """
#     获取142891263425277DZ1 sx_func_tree_directory 未分类的id, 替代用力列表未分类的TREE_DIRECTORY_ID
#     """
#     conn, cursor = getConnected()
#     try:
#         DIRECTORY_NAME = u"未分类"
#         sql = "select TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY where SYSTEM_ID='142891263425277DZ1' " \
#               "AND DIRECTORY_NAME='%s'"%DIRECTORY_NAME
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         TREE_DIRECTORY_ID = 0
#         if result and result[0][0]:
#             TREE_DIRECTORY_ID = result[0][0]
#         if TREE_DIRECTORY_ID:
#             sql = "update SX_CASES set TREE_DIRECTORY_ID=%s where SYSTEM_ID='142891263425277DZ1' " \
#                   "AND TREE_DIRECTORY_ID=0"%TREE_DIRECTORY_ID
#             cursor.execute(sql)
#     except Exception as e:
#         print e
#     finally:
#         conn.commit()
#         cursor.close()
#         conn.close()

def update_sx_func_tree_directory_is_0():
    """
    获取142891263425277DZ1 sx_func_tree_directory 未分类的id, 替代用力列表未分类的TREE_DIRECTORY_ID
    """
    conn, cursor = getConnected()
    try:
        DIRECTORY_NAME = u"未分类"
        sql = "select TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY where SYSTEM_ID='142891263425277DZ1' " \
              "AND DIRECTORY_NAME='%s'" % DIRECTORY_NAME
        cursor.execute(sql)
        result = cursor.fetchall()
        TREE_DIRECTORY_ID = 0
        if result and result[0][0]:
            TREE_DIRECTORY_ID = result[0][0]
        if TREE_DIRECTORY_ID:
            sql = "update SX_CASES set TREE_DIRECTORY_ID=%s where SYSTEM_ID='142891263425277DZ1' " \
                  "AND TREE_DIRECTORY_ID=0" % TREE_DIRECTORY_ID
            cursor.execute(sql)
        sql = "select ID, FUNCID from SX_INTERFACE_INFO where SYSTEM_ID='142891263425277DZ1'"
        # sql = "select CASES_DETAIL_ID, FUNCID from sx_cases_detail where cases_id in (select cases_id from sx_cases WHERE  SYSTEM_ID='142891263425277DZ1')"
        cursor.execute(sql)
        count = 0
        for row in cursor.fetchall():
            count +=1
            print "running=%s"%count
            ID, FUNCID = row
            FUNCID = FUNCID.strip()
            sql = "update sx_cases_detail set PARAM_ID=%s where FUNCID='%s' and cases_id in (select cases_id from sx_cases WHERE  SYSTEM_ID='142891263425277DZ1')" % ( ID, FUNCID)
            cursor.execute(sql)
    except Exception as e:
        print e
    finally:
        conn.commit()
        cursor.close()
        conn.close()

def orderby_TREE_DIRECTORY_ID_to_get_funcid(servertype, itempos, TREE_DIRECTORY_ID, FUNCID, data_TREE_DIRECTORY_ID):
    """
    i['servertype'], i['itempos']
    orderby_TREE_DIRECTORY_ID_to_get_funcid
    """
    conn, cursor = getConnected()
    try:
        if FUNCID.startswith('4'):
            pass
        IDs = []
        TREE_DIRECTORY_ID_dict = {}
        sql = "select ID, TREE_DIRECTORY_ID from SX_INTERFACE_INFO where SYSTEM_ID='142891263425277DZ1' and FUNCID='%s'"%(FUNCID)
        cursor.execute(sql)
        for row in cursor.fetchall():
            ID, TREE_DIRECTORY_ID = row
            IDs.append(ID)
            TREE_DIRECTORY_ID_dict[str(ID)] = TREE_DIRECTORY_ID
        if len(IDs) == 0:
            return ""
        if len(IDs) >1:
            for ID in IDs:
                TREE_DIRECTORY_ID = TREE_DIRECTORY_ID_dict[str(ID)]
                itempos_type = data_TREE_DIRECTORY_ID[str(TREE_DIRECTORY_ID)]
                _type = itempos_type.split("=")[1]
                _itempos = itempos_type.split("=")[0]
                if _type ==str(servertype) and _itempos==str(itempos):
                    return ID
        else:
            return IDs[0]
    except Exception as e:
        print e
    finally:
        conn.commit()
        cursor.close()
        conn.close()
def select():
    conn, cursor = getConnected()
    sql = "select max(CASES_ID) from SX_CASES"
    cursor.execute(sql)
    global CASES_ID
    CASES_ID = cursor.fetchall()
    CASES_ID = int(CASES_ID[0][0])
    sql = "ALTER TABLE SX_CASES ALTER COLUMN CASES_ID RESTART WITH %s" % (CASES_ID + 1)
    cursor.execute(sql)

    sql = "select max(CASES_DETAIL_ID) from SX_CASES_DETAIL"
    cursor.execute(sql)
    global CASES_DETAIL_ID
    CASES_DETAIL_ID = cursor.fetchall()
    CASES_DETAIL_ID = int(CASES_DETAIL_ID[0][0])
    sql = "ALTER TABLE SX_CASES_DETAIL ALTER COLUMN CASES_DETAIL_ID RESTART WITH %s" % (CASES_DETAIL_ID + 1)
    cursor.execute(sql)

    sql = "select max(PARAM_DATA_ID) from SX_CASES_DETAIL_PARAM_DATA"
    cursor.execute(sql)
    global PARAM_DATA_ID
    PARAM_DATA_ID = cursor.fetchall()
    PARAM_DATA_ID = int(PARAM_DATA_ID[0][0])
    sql = "ALTER TABLE SX_CASES_DETAIL_PARAM_DATA ALTER COLUMN PARAM_DATA_ID RESTART WITH %s" % (PARAM_DATA_ID + 1)
    cursor.execute(sql)

    sql = "select max(CASES_PARAM_DATA_ID) from SX_CASES_PARAM_DATA"
    cursor.execute(sql)
    global CASES_PARAM_DATA_ID
    CASES_PARAM_DATA_ID = cursor.fetchall()
    CASES_PARAM_DATA_ID = int(CASES_PARAM_DATA_ID[0][0])
    sql = "ALTER TABLE SX_CASES_PARAM_DATA ALTER COLUMN CASES_PARAM_DATA_ID RESTART WITH %s" % (CASES_PARAM_DATA_ID + 1)
    cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()


def delete():
    conn, cursor = getConnected()
    sql = "DELETE from SX_CASES_PARAM_DATA where cases_id in (select cases_id from sx_cases WHERE SYSTEM_ID='142891263425277DZ1');"
    cursor.execute(sql)
    sql = "DELETE from SX_CASES_DETAIL_PARAM_DATA WHERE CASES_DETAIL_ID IN (select CASES_DETAIL_ID from sx_cases_detail where cases_id in (select cases_id from sx_cases WHERE SYSTEM_ID='142891263425277DZ1'));"
    cursor.execute(sql)
    cases_ids = []
    sql ="select cases_id from sx_cases WHERE SYSTEM_ID='142891263425277DZ1'"
    cursor.execute(sql)
    for row in cursor.fetchall():
        cases_ids.append(str(row[0]))
    cases_ids = [cases_ids[i:i + 1000] for i in range(0, len(cases_ids), 1000)]
    for case_str in cases_ids:
        case_str = ",".join(case_str)
        sql = "DELETE from sx_cases_detail where cases_id in ("+case_str+")"
        cursor.execute(sql)
        conn.commit()


    sql = "DELETE from sx_cases WHERE SYSTEM_ID='142891263425277DZ1';"
    cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()
def update_old_table_interface():
    # conn = getDbconnection_zhlc()
    # cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    # sql = """select funcid,casename, cmdstring from tbl_cases GROUP by funcid"""
    # cursor.execute(sql)
    # result_tbl_cases = cursor.fetchall()
    # cursor.close()
    # conn.close()
    FUNCID_CASES_DETAIL_ID = {}
    conn, cursor = getConnected()
    sql = "select FUNCID, CASES_DETAIL_ID from SX_CASES_DETAIL where CASES_ID in (select CASES_ID from SX_CASES where  SYSTEM_ID='142891263425277DZ1')"
    cursor.execute(sql)
    rs = cursor.fetchall()
    for i in rs:
        FUNCID = i[0]
        CASES_DETAIL_ID = i[1]
        if FUNCID_CASES_DETAIL_ID:
            if FUNCID not in FUNCID_CASES_DETAIL_ID.keys():
                FUNCID_CASES_DETAIL_ID[FUNCID] = [CASES_DETAIL_ID]
            else:
                FUNCID_CASES_DETAIL_ID[FUNCID].append(CASES_DETAIL_ID)
        else:
            FUNCID_CASES_DETAIL_ID[str(FUNCID)] = [CASES_DETAIL_ID]

    CASES_DETAIL_ID_PARAM_DATA_ID = {}
    sql = "select CASES_DETAIL_ID, PARAM_DATA, PARAM_DATA_ID from SX_CASES_DETAIL_PARAM_DATA where CASES_DETAIL_ID in  (select CASES_DETAIL_ID from SX_CASES_DETAIL where CASES_ID in (select CASES_ID from SX_CASES where  SYSTEM_ID='142891263425277DZ1'))"

    cursor.execute(sql)
    rs = cursor.fetchall()
    for i in rs:
        CASES_DETAIL_ID_PARAM_DATA_ID[i[0]] = i[1]

    f_field = {}
    for FUNCID in FUNCID_CASES_DETAIL_ID:
        CASES_DETAIL_IDS = FUNCID_CASES_DETAIL_ID[FUNCID]
        for CASES_DETAIL_ID in CASES_DETAIL_IDS:
            PARAM_DATA = CASES_DETAIL_ID_PARAM_DATA_ID[CASES_DETAIL_ID]
            field = [p.split("=")[0].strip() for p in PARAM_DATA.split("#")]

            if f_field:
                if FUNCID not in f_field.keys():
                    f_field[FUNCID] = field
                else:
                    l = f_field[FUNCID]
                    l.extend(field)
                    l = list(set(l))
                    # l.pop(u'帐号组编号')
                    f_field[FUNCID] = l
            else:
                f_field[FUNCID] = field

    # return
    args = (u'未分类', "142891263425277DZ1")
    sql = "select TREE_DIRECTORY_ID from SX_INTERFACE_TREE_DIRECTORY where DIRECTORY_NAME='%s' and SYSTEM_ID='%s' and PARENT_ID=0" % args
    cursor.execute(sql)
    rs = cursor.fetchall()
    TREE_DIRECTORY_ID = rs[0][0]


    sql = "select "
    sql = "select max(ID) from SX_INTERFACE_INFO"
    cursor.execute(sql)
    ID = cursor.fetchall()
    INTERFACE_ID = int(ID[0][0])
    COUNT = 0
    for funcid in f_field:
        COUNT += 1
        print COUNT
        if not funcid or funcid == None or funcid == 'null':
            continue
        # funcid:410221,count:10,trdpwd:@trdpwd@,netaddr:0,operway:4,ext:0,orgid:@orgid@,qryflag:0
        casename = funcid
        FUNCID = funcid

        fields = f_field[funcid]

        sql = "SELECT ID FROM SX_INTERFACE_INFO WHERE SYSTEM_ID= '142891263425277DZ1' and FUNCID= '%s'" % FUNCID
        cursor.execute(sql)
        result = cursor.fetchall()
        flag = True
        if result:
            if result[0]:
                sql = "SELECT count(*) FROM SX_INTERFACE_DETAIL_INFO WHERE  INTERFACE_ID  = %s" % result[0][0]
                cursor.execute(sql)
                result1 = cursor.fetchall()
                if result1[0][0]:
                    sql = "delete from SX_INTERFACE_DETAIL_INFO where INTERFACE_ID  = %s" % result[0][0]
                    cursor.execute(sql)
                    PARAM_INDEX = 0
                    for PARAM_NAME in fields:
                        if not PARAM_NAME:
                            continue
                        PARAM_INDEX += 1
                        sql = "insert into SX_INTERFACE_DETAIL_INFO(INTERFACE_ID, INPUT_TYPE, PARAM_NAME, PARAM_INDEX, PARAM_DESC) " \
                              "values(%s, '%s','%s', %s  ,'%s')" % (result[0][0], 'IN', PARAM_NAME, PARAM_INDEX, PARAM_NAME)
                        cursor.execute(sql)
                    flag = False
                else:
                    sql = "delete from SX_INTERFACE_INFO where SYSTEM_ID= '142891263425277DZ1' and FUNCID= '%s'" % FUNCID
                    cursor.execute(sql)
        if flag == True:
            INTERFACE_ID += 1
            sql = "insert into SX_INTERFACE_INFO(INTERFACE_NAME, FUNCID, FUNCID_DESC, TREE_DIRECTORY_ID, SYSTEM_ID, INTERFACE_DESC) " \
                  "values('%s', '%s', '%s', %s ,'%s' ,'%s')" % (
                      casename, FUNCID, casename, TREE_DIRECTORY_ID, "142891263425277DZ1", casename)
            cursor.execute(sql)
            conn.commit()
            PARAM_INDEX = 0
            for PARAM_NAME in fields:
                if not PARAM_NAME:
                    continue
                PARAM_INDEX += 1
                sql = "insert into SX_INTERFACE_DETAIL_INFO(INTERFACE_ID, INPUT_TYPE, PARAM_NAME, PARAM_INDEX, PARAM_DESC) " \
                      "values(%s, '%s','%s', %s  ,'%s')" % (INTERFACE_ID, 'IN', PARAM_NAME, PARAM_INDEX, PARAM_NAME)
                cursor.execute(sql)
                conn.commit()

    # null更新为空
    sql = """update SX_INTERFACE_DETAIL_INFO set PARAM_TYPE ='' where INTERFACE_ID in (select ID from SX_INTERFACE_INFO where  SYSTEM_ID = '142891263425277DZ1' ) AND PARAM_TYPE IS NULL"""
    cursor.execute(sql)

    sql = "update SX_INTERFACE_INFO set  ADAPTER_TYPE=''  where SYSTEM_ID = '142891263425277DZ1' AND ADAPTER_TYPE IS NULL"
    cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()
def del_many_funcid_interface_info():
    conn, cursor = getConnected()
    sql = "update SX_INTERFACE_DETAIL_INFO set INPUT_TYPE = 'IN' WHERE INPUT_TYPE = 'in'"
    cursor.execute(sql)
    sql = "update SX_INTERFACE_DETAIL_INFO set INPUT_TYPE = 'OUT' WHERE INPUT_TYPE = 'out'"
    cursor.execute(sql)

    sql = "SELECT T.FUNCID FROM (select FUNCID, COUNT(FUNCID) AS NUM from SX_INTERFACE_INFO where " \
          "SYSTEM_ID = '142891263425277DZ1' GROUP BY FUNCID) T  WHERE T.NUM >=2"
    cursor.execute(sql)
    result = cursor.fetchall()
    for i in result:
        sql = "select ID from SX_INTERFACE_INFO where SYSTEM_ID = '142891263425277DZ1' AND FUNCID = '%s'" % i[0]
        cursor.execute(sql)
        ids = cursor.fetchall()
        id_list = []
        for j in ids:
            id_list.append(str(j[0]))
        id_str = ",".join(id_list)
        sql = "select INTERFACE_ID, count(INTERFACE_ID) as num from SX_INTERFACE_DETAIL_INFO where INTERFACE_ID in (" + id_str + ") group by INTERFACE_ID"
        cursor.execute(sql)
        max_interface_id = cursor.fetchall()
        dictt_interface = {}
        for id in id_list:
            dictt_interface[int(id)] = 0
        for id in max_interface_id:
            dictt_interface[int(id[0])] = id[1]
        print dictt_interface
        values = dictt_interface.values()
        if len(values) > 1 and len(list(set(values))) == 1:
            print values
            print "single"
            id_list.pop()
        else:
            max_values = max(values)
            for key in dictt_interface:
                if int(dictt_interface[key]) == max_values:
                    id_list.remove(str(key))
                    break
        id_list_str = ",".join(id_list)
        sql = "delete from SX_INTERFACE_DETAIL_INFO where INTERFACE_ID in (" + id_list_str + ")"
        cursor.execute(sql)
        sql = "delete from SX_INTERFACE_INFO where id in (" + id_list_str + ")"
        cursor.execute(sql)
        print sql
    conn.commit()
    cursor.close()
    conn.close()


def get_new_tree_sx_func_tree_directory():
    conn, cursor = getConnected()
    sql = "select max(TREE_DIRECTORY_ID) from SX_FUNC_TREE_DIRECTORY"
    cursor.execute(sql)
    rs = cursor.fetchall()
    max_id = rs[0][0]
    max_id = max_id + 1
    sql = u"select TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '根节点'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    PARENT_ID = rs[0][0]

    sql = "insert into SX_FUNC_TREE_DIRECTORY(TREE_DIRECTORY_ID,DIRECTORY_NAME,DIRECTORY_TYPE,PARENT_ID,IS_LEAF,INDEX_NUM,TREE_STATE,SYSTEM_ID) VALUES (%s, '%s', '%s', %s, %s, %s,%s, '%s')" % (max_id, u"集中交易案例", "DIRECTORY_LEVEL_1", PARENT_ID, 0, 1, 1, "142891263425277DZ1")
    cursor.execute(sql)

    sql = "select TREE_DIRECTORY_ID,DIRECTORY_TYPE from SX_FUNC_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1'and DIRECTORY_TYPE !=" \
          " 'DIRECTORY_LEVEL_0' and TREE_DIRECTORY_ID!=%s"%max_id
    cursor.execute(sql)
    for row in cursor.fetchall():
        TREE_DIRECTORY_ID, DIRECTORY_TYPE = row
        pre = DIRECTORY_TYPE[:-1]
        pro = DIRECTORY_TYPE[-1:]
        pro = int(pro)
        if pro > 0:
            pro = pro + 1
        if DIRECTORY_TYPE == "DIRECTORY_LEVEL_1":
            NEW_DIRECTORY_TYPE = pre + str(pro)
            sql = "update SX_FUNC_TREE_DIRECTORY set DIRECTORY_TYPE = '%s' , PARENT_ID= %s where TREE_DIRECTORY_ID=%s"%(NEW_DIRECTORY_TYPE, max_id, TREE_DIRECTORY_ID)
            cursor.execute(sql)
        else:
            NEW_DIRECTORY_TYPE = pre + str(pro)
            sql = "update SX_FUNC_TREE_DIRECTORY set DIRECTORY_TYPE = '%s' where TREE_DIRECTORY_ID=%s" % (
            NEW_DIRECTORY_TYPE, TREE_DIRECTORY_ID)
            cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()
def interface_get_new_tree_sx_func_tree_directory():
    conn, cursor = getConnected()
    sql = "select max(TREE_DIRECTORY_ID) from SX_INTERFACE_TREE_DIRECTORY"
    cursor.execute(sql)
    rs = cursor.fetchall()
    max_id = rs[0][0]
    max_id = max_id + 1
    sql = u"select TREE_DIRECTORY_ID from SX_INTERFACE_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '根节点'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    PARENT_ID = rs[0][0]

    sql = "insert into SX_INTERFACE_TREE_DIRECTORY(TREE_DIRECTORY_ID,DIRECTORY_NAME,DIRECTORY_TYPE,PARENT_ID,IS_LEAF,INDEX_NUM,TREE_STATE,SYSTEM_ID) VALUES (%s, '%s', '%s', %s, %s, %s,%s, '%s')" % (max_id, u"集中交易接口", "DIRECTORY_LEVEL_1", PARENT_ID, 0, 1, 1, "142891263425277DZ1")
    cursor.execute(sql)

    sql = "select TREE_DIRECTORY_ID,DIRECTORY_TYPE from SX_INTERFACE_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1'and DIRECTORY_TYPE !=" \
          " 'DIRECTORY_LEVEL_0' and TREE_DIRECTORY_ID!=%s"%max_id
    cursor.execute(sql)
    for row in cursor.fetchall():
        TREE_DIRECTORY_ID, DIRECTORY_TYPE = row
        pre = DIRECTORY_TYPE[:-1]
        pro = DIRECTORY_TYPE[-1:]
        pro = int(pro)
        if pro > 0:
            pro = pro + 1
        if DIRECTORY_TYPE == "DIRECTORY_LEVEL_1":
            NEW_DIRECTORY_TYPE = pre + str(pro)
            sql = "update SX_INTERFACE_TREE_DIRECTORY set DIRECTORY_TYPE = '%s' , PARENT_ID= %s where TREE_DIRECTORY_ID=%s"%(NEW_DIRECTORY_TYPE, max_id, TREE_DIRECTORY_ID)
            cursor.execute(sql)
        else:
            NEW_DIRECTORY_TYPE = pre + str(pro)
            sql = "update SX_INTERFACE_TREE_DIRECTORY set DIRECTORY_TYPE = '%s' where TREE_DIRECTORY_ID=%s" % (
            NEW_DIRECTORY_TYPE, TREE_DIRECTORY_ID)
            cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()
def case_convert():
    conn, cursor = getConnected()
    #先查询QS根节点节点
    sql = u"select TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '根节点'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    Z1_GEN_TREE_DIRECTORY_ID = rs[0][0]

    # 先查询jzjy根节点节点
    sql = u"select TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '未分类'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    Z1_WEIFENLEI_TREE_DIRECTORY_ID = rs[0][0]

    ###################################
    # 先查询QS根节点节点
    sql = u"select TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '根节点'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    JA_GEN_TREE_DIRECTORY_ID = rs[0][0]

    # 先查询jzjy根节点节点
    sql = u"select TREE_DIRECTORY_ID from SX_FUNC_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '未分类'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    JA_WEIFENLEI_TREE_DIRECTORY_ID = rs[0][0]

    # #先更新案例树的 根目录节点
    # sql = "update SX_CASES set TREE_DIRECTORY_ID = %s where SYSTEM_ID = '142891263425277DZ1' and TREE_DIRECTORY_ID=%s"%(Z1_GEN_TREE_DIRECTORY_ID, JA_GEN_TREE_DIRECTORY_ID)
    # cursor.execute(sql)
    # 更新案例树的 未分类节点节点
    sql = "update SX_CASES set TREE_DIRECTORY_ID = %s where SYSTEM_ID = '142891263425277DZ1' and TREE_DIRECTORY_ID=%s" % (
        Z1_WEIFENLEI_TREE_DIRECTORY_ID, JA_WEIFENLEI_TREE_DIRECTORY_ID)
    cursor.execute(sql)

    #案例数挂载进去
    sql = "update SX_FUNC_TREE_DIRECTORY set PARENT_ID = %s where SYSTEM_ID = '142891263425277DZ1' and PARENT_ID=%s"%(Z1_GEN_TREE_DIRECTORY_ID, JA_GEN_TREE_DIRECTORY_ID)
    cursor.execute(sql)

    sql = "update SX_CASES set SYSTEM_ID = '142891263425277DZ1' where SYSTEM_ID = '142891263425277DZ1'"
    cursor.execute(sql)

    sql = "DELETE FROM SX_FUNC_TREE_DIRECTORY where SYSTEM_ID = '142891263425277DZ1' AND TREE_DIRECTORY_ID in (%s, %s)"%(JA_GEN_TREE_DIRECTORY_ID, JA_WEIFENLEI_TREE_DIRECTORY_ID)
    cursor.execute(sql)

    sql = "update SX_FUNC_TREE_DIRECTORY set SYSTEM_ID = '142891263425277DZ1' where SYSTEM_ID = '142891263425277DZ1'"
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def interface_convert():
    conn, cursor = getConnected()
    #先查询QS根节点节点
    sql = u"select TREE_DIRECTORY_ID from SX_INTERFACE_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '根节点'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    Z1_GEN_TREE_DIRECTORY_ID = rs[0][0]

    # 先查询jzjy根节点节点
    sql = u"select TREE_DIRECTORY_ID from SX_INTERFACE_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '未分类'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    Z1_WEIFENLEI_TREE_DIRECTORY_ID = rs[0][0]

    ###################################
    # 先查询QS根节点节点
    sql = u"select TREE_DIRECTORY_ID from SX_INTERFACE_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '根节点'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    JA_GEN_TREE_DIRECTORY_ID = rs[0][0]

    # 先查询jzjy根节点节点
    sql = u"select TREE_DIRECTORY_ID from SX_INTERFACE_TREE_DIRECTORY WHERE SYSTEM_ID = '142891263425277DZ1' and DIRECTORY_NAME= '未分类'"
    cursor.execute(sql)
    rs = cursor.fetchall()
    JA_WEIFENLEI_TREE_DIRECTORY_ID = rs[0][0]

    sql = "update SX_INTERFACE_INFO set TREE_DIRECTORY_ID = %s where SYSTEM_ID = '142891263425277DZ1' and TREE_DIRECTORY_ID=%s" % (
        Z1_WEIFENLEI_TREE_DIRECTORY_ID, JA_WEIFENLEI_TREE_DIRECTORY_ID)
    cursor.execute(sql)

    sql = "update SX_INTERFACE_INFO set TIMEOUT = 101 where SYSTEM_ID = '142891263425277DZ1'"
    cursor.execute(sql)

    sql = "update SX_INTERFACE_INFO set SYSTEM_ID = '142891263425277DZ1' where SYSTEM_ID = '142891263425277DZ1'"
    cursor.execute(sql)
    #案例数挂载进去
    sql = "update SX_INTERFACE_TREE_DIRECTORY set PARENT_ID = %s where SYSTEM_ID = '142891263425277DZ1' and PARENT_ID=%s"%(Z1_GEN_TREE_DIRECTORY_ID, JA_GEN_TREE_DIRECTORY_ID)
    cursor.execute(sql)

    sql = "DELETE FROM SX_INTERFACE_TREE_DIRECTORY where SYSTEM_ID = '142891263425277DZ1' AND TREE_DIRECTORY_ID in (%s, %s)" % (
    JA_GEN_TREE_DIRECTORY_ID, JA_WEIFENLEI_TREE_DIRECTORY_ID)
    cursor.execute(sql)

    sql = "update SX_INTERFACE_TREE_DIRECTORY set SYSTEM_ID = '142891263425277DZ1' where SYSTEM_ID = '142891263425277DZ1'"
    cursor.execute(sql)

    conn.commit()
    cursor.close()
    conn.close()

def split_test(args):
    # a = "a:,1,b:2,1,2,3,c:3"
    args = args.split(",")
    new_word = []
    w = ""
    for i in args:
        if ":" in i:
            new_word.append(i)
            w=""
        else:
            if new_word:
                str = new_word[-1]
                str = str+","+i
                new_word[-1] = str
    n_c = []
    keys = []
    for i in new_word:
        num = i.count(":")
        if num == 1:
            key = i.split(":")[0]
            keys.append(key)
            i = i.replace(",","&comma&")
            n_c.append(i)
        else:
            index = i.index(":")
            key = i[:index+1]
            value = i[index+1:].replace(":", "&colon&")
            i = key+value

            key = i.split(":")[0]
            keys.append(key)
            i = i.replace(",", "&comma&")
            n_c.append(i)
    n_c_str = ",".join(n_c)
    keys_str = "=#".join(keys) + "="
    print n_c_str, keys_str
    return n_c_str, keys_str

def orderby_interface_field():
    try:
        conn, cursor = getConnected()
        sql = "SELECT FUNCID FROM SX_CASES_DETAIL WHERE CASES_ID IN (SELECT CASES_ID FROM SX_CASES WHERE SYSTEM_ID='1428912311065GCAO2')"
        cursor.execute(sql)
        rs = cursor.fetchall()
        FUNCIDS = []
        if rs:
            for i in rs:
                FUNCID = i[0]
                if FUNCID:
                    FUNCID = "'" + str(FUNCID) + "'"
                    FUNCIDS.append(FUNCID)

        FUNCIDStr = ",".join(list(set(FUNCIDS)))
        sql = "select ID, FUNCID FROM SX_INTERFACE_INFO WHERE SYSTEM_ID = '142891263425277DZ1' and FUNCID in ("+FUNCIDStr+")"
        cursor.execute(sql)
        rs = cursor.fetchall()
        interface_info = {}
        if rs:
            for i in rs:
                ID = i[0]
                FUNCID = i[1]
                sql = "update SX_CASES_DETAIL set PARAM_ID=%s where FUNCID='%s' AND CASES_ID IN (SELECT CASES_ID FROM SX_CASES WHERE SYSTEM_ID='1428912311065GCAO2')"%(ID, FUNCID)
                cursor.execute(sql)

                interface_filed = []
                sql = "select PARAM_NAME FROM SX_INTERFACE_DETAIL_INFO WHERE INTERFACE_ID =%s order by PARAM_INDEX"%ID
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    for j in result:
                        interface_filed.append(j[0].strip())
                if interface_filed:
                    interface_info[str(FUNCID)] = interface_filed

        sql = "select PARAM_DATA, PARAM_DATA_ID, CASES_DETAIL_ID FROM SX_CASES_DETAIL_PARAM_DATA WHERE CASES_DETAIL_ID IN (SELECT CASES_DETAIL_ID FROM SX_CASES_DETAIL WHERE CASES_ID IN (SELECT CASES_ID FROM SX_CASES WHERE SYSTEM_ID='1428912311065GCAO2'))"
        cursor.execute(sql)
        PARAM_DATAS = cursor.fetchall()
        CASES_DETAIL_ID_PARAM_DATA = {}
        if PARAM_DATAS:
            for i in PARAM_DATAS:
                CASES_DETAIL_ID = i[2]
                CASES_DETAIL_ID_PARAM_DATA[CASES_DETAIL_ID] = i

        sql = "select CASES_DETAIL_ID, FUNCID FROM SX_CASES_DETAIL WHERE CASES_ID IN (SELECT CASES_ID FROM SX_CASES WHERE SYSTEM_ID='1428912311065GCAO2')"
        cursor.execute(sql)
        rs = cursor.fetchall()
        FUNCID_CASES_DETAIL_ID = {}
        if rs:
            for i in rs:
                CASES_DETAIL_ID = i[0]
                FUNCID = str(i[1])
                if FUNCID_CASES_DETAIL_ID:
                    if FUNCID not in FUNCID_CASES_DETAIL_ID.keys():
                        FUNCID_CASES_DETAIL_ID[FUNCID] = [CASES_DETAIL_ID]
                    else:
                        FUNCID_CASES_DETAIL_ID[FUNCID].append(CASES_DETAIL_ID)
                else:
                    FUNCID_CASES_DETAIL_ID[FUNCID] = [CASES_DETAIL_ID]

        count = 0
        result = []
        for FUNCID in interface_info:
            CASES_DETAIL_IDs = []
            count +=1
            print count
            fields = interface_info[FUNCID]
            if str(FUNCID) in FUNCID_CASES_DETAIL_ID.keys():
                CASES_DETAIL_IDs = FUNCID_CASES_DETAIL_ID[str(FUNCID)]
            if CASES_DETAIL_IDs:
                for CASES_DETAIL_ID in CASES_DETAIL_IDs:
                    if CASES_DETAIL_ID in CASES_DETAIL_ID_PARAM_DATA.keys():
                        PARAM_DATAS = CASES_DETAIL_ID_PARAM_DATA[CASES_DETAIL_ID]

                        s_data = []
                        PARAM_DATA = PARAM_DATAS[0]
                        PARAM_DATA_ID = PARAM_DATAS[1]
                        k_v_list = PARAM_DATA.strip().split("#")
                        k_v_dict = {}
                        for k_v in k_v_list:
                            if k_v:
                                k = k_v.split("=")[0].strip()
                                v = k_v.split("=")[1]
                                k_v_dict[k] = v
                        for field in fields:
                            if field:
                                if field in k_v_dict.keys():
                                    value = k_v_dict[field]
                                    s = field + "=" + value
                                else:
                                    s = field + "="
                                s_data.append(s)
                        s_d = "#".join(s_data)
                        data = (s_d, PARAM_DATA_ID)
                        result.append(data)
        COUNT = 0
        args = [result[i:i + 100] for i in range(0, len(result), 100)]
        for arg in args:
            COUNT += 1
            print COUNT
            sql = "update SX_CASES_DETAIL_PARAM_DATA set PARAM_DATA=? where PARAM_DATA_ID=? "
            cursor.executemany(sql, arg)
        print "exe end______________________________"
        conn.commit()
        cursor.close()
        conn.close()
        print "success______________________________"
    except Exception as e:
        print "error  ______________________________"

def update_param_data():
    funcid_CASES_DETAIL_ID_dict = {}
    FUNCID_CASES_DETAIL_ID = {}
    conn, cursor = getConnected()
    sql = "select FUNCID, CASES_DETAIL_ID from SX_CASES_DETAIL where CASES_ID in (select CASES_ID from SX_CASES where  SYSTEM_ID='142891263425277DZ1')"
    cursor.execute(sql)
    rs = cursor.fetchall()
    for i in rs:

        FUNCID = i[0]
        CASES_DETAIL_ID = i[1]
        funcid_CASES_DETAIL_ID_dict[str(CASES_DETAIL_ID)] = FUNCID
        if FUNCID_CASES_DETAIL_ID:
            if FUNCID not in FUNCID_CASES_DETAIL_ID.keys():
                FUNCID_CASES_DETAIL_ID[FUNCID] = [CASES_DETAIL_ID]
            else:
                FUNCID_CASES_DETAIL_ID[FUNCID].append(CASES_DETAIL_ID)
        else:
            FUNCID_CASES_DETAIL_ID[str(FUNCID)] = [CASES_DETAIL_ID]

    CASES_DETAIL_ID_PARAM_DATA_ID = {}
    sql = "select CASES_DETAIL_ID, PARAM_DATA, PARAM_DATA_ID from SX_CASES_DETAIL_PARAM_DATA where CASES_DETAIL_ID in  (select CASES_DETAIL_ID from SX_CASES_DETAIL where CASES_ID in (select CASES_ID from SX_CASES where  SYSTEM_ID='142891263425277DZ1'))"

    cursor.execute(sql)
    rs = cursor.fetchall()
    for i in rs:
        CASES_DETAIL_ID_PARAM_DATA_ID[i[0]] = i[1]

    f_field = {}
    for FUNCID in FUNCID_CASES_DETAIL_ID:
        CASES_DETAIL_IDS = FUNCID_CASES_DETAIL_ID[FUNCID]
        for CASES_DETAIL_ID in CASES_DETAIL_IDS:
            PARAM_DATA = CASES_DETAIL_ID_PARAM_DATA_ID[CASES_DETAIL_ID]
            field = [p.split("=")[0].strip() for p in PARAM_DATA.split("#")]

            if f_field:
                if FUNCID not in f_field.keys():
                    f_field[FUNCID] = field
                else:
                    l = f_field[FUNCID]
                    l.extend(field)
                    l = list(set(l))
                    # l.pop(u'帐号组编号')
                    f_field[FUNCID] = l
            else:
                f_field[FUNCID] = field

    sql = "select CASES_DETAIL_ID, PARAM_DATA, PARAM_DATA_ID from SX_CASES_DETAIL_PARAM_DATA where CASES_DETAIL_ID in  (select CASES_DETAIL_ID from SX_CASES_DETAIL where CASES_ID in (select CASES_ID from SX_CASES where  SYSTEM_ID='142891263425277DZ1'))"

    cursor.execute(sql)
    rs = cursor.fetchall()
    result = []
    count = 0
    try:
        for i in rs:
            count+=1
            print count
            s_data = []
            PARAM_DATA = i[1]
            CASES_DETAIL_ID = i[0]
            PARAM_DATA_ID = i[2]
            funcid = funcid_CASES_DETAIL_ID_dict[str(CASES_DETAIL_ID)]
            fields = f_field[funcid]
            k_v_list = PARAM_DATA.strip().split("#")
            k_v_dict = {}
            for k_v in k_v_list:
                if k_v:
                    k = k_v.split("=")[0].strip()
                    v = k_v.split("=")[1]
                    k_v_dict[k] = v
            for field in fields:
                if field:
                    if field in k_v_dict.keys():
                        value = k_v_dict[field]
                        s = field +"="+value
                    else:
                        s = field + "="
                    s_data.append(s)
            s_d = "#".join(s_data)
            # result[PARAM_DATA_ID] = s_d

            data = (s_d, PARAM_DATA_ID)
            result.append(data)
        COUNT = 0
        args = [result[i:i + 100] for i in range(0, len(result), 100)]
        for arg in args:
            COUNT += 1
            print COUNT
            sql = "update SX_CASES_DETAIL_PARAM_DATA set PARAM_DATA=? where PARAM_DATA_ID=? "
            # print sql
            cursor.executemany(sql, arg)
            # cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        print "SX_CASES_DETAIL_PARAM_DATA"
    except:
        print "error"


if __name__ == '__main__':
    orderby_interface_field()
