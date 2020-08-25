import time
import datetime
import logging
import yaml
import json
import zmq
import zmq.auth
from zmq.utils.strtypes import b
try:
    from lava_dispatcher.utils.constants import INTERNAL_RESULTS_SOCKET
except ImportError:
    INTERNAL_RESULTS_SOCKET = "ipc:///tmp/lava.results"


class ZMQPushHandler(logging.Handler):
    def __init__(self, logging_url, master_cert, slave_cert, job_id, ipv6):
        super(ZMQPushHandler, self).__init__()

        # Keep track of the parameters
        self.logging_url = logging_url
        self.master_cert = master_cert
        self.slave_cert = slave_cert
        self.ipv6 = ipv6

        # Create the PUSH socket
        # pylint: disable=no-member
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.setsockopt(zmq.SNDHWM, 5000)
        self.socket.setsockopt(zmq.SNDTIMEO, 30)

        # Push socket to send action result messgae to slave
        # add by xwx247599
        self.push_socket = self.context.socket(zmq.PUSH)
        self.push_socket.connect(INTERNAL_RESULTS_SOCKET)
        # add end

        if ipv6:
            self.socket.setsockopt(zmq.IPV6, 1)

        # Load the certificates (if encryption is on)
        if master_cert is not None and slave_cert is not None:
            (client_public, client_private) = zmq.auth.load_certificate(slave_cert)
            self.socket.curve_publickey = client_public
            self.socket.curve_secretkey = client_private

            (server_public, _) = zmq.auth.load_certificate(master_cert)
            self.socket.curve_serverkey = server_public

        self.socket.connect(logging_url)

        self.job_id = str(job_id)
        self.formatter = logging.Formatter("%(message)s")

    def emit(self, record):
        msg = [b(self.job_id), b(self.formatter.format(record))]
        try:
            self.socket.send_multipart(msg)
        except zmq.error.Again:
            pass

    def send_result(self, msg):
        message = [b(self.job_id), b(msg)]
        self.push_socket.send_multipart(message)

    def close(self, linger=-1):
        # If the process crashes really early, the handler will be closed
        # directly by the logging module. In this case, close is called without
        # any arguments.
        super(ZMQPushHandler, self).close()
        self.context.destroy(linger=linger)


class YAMLLogger(logging.Logger):
    def __init__(self, name):
        super(YAMLLogger, self).__init__(name)
        self.handler = None

    def addZMQHandler(self, logging_url, master_cert, slave_cert, job_id, ipv6):
        self.handler = ZMQPushHandler(logging_url, master_cert,
                                      slave_cert, job_id, ipv6)
        self.addHandler(self.handler)
        return self.handler

    def close(self, linger=-1):
        if self.handler is not None:
            self.handler.close(linger)
            self.removeHandler(self.handler)
            self.handler = None

    def log_message(self, level, level_name, message, *args, **kwargs):  # pylint: disable=unused-argument
        #lwx878996, 2020/1/19 add start
        if self.handler is None:
            return
        #lwx878996, 2020/1/19 add end
        # Build the dictionnary
        data = {'dt': datetime.datetime.now().isoformat()[0:19].replace('T',' '),
                'lvl': level_name}

        if level_name == 'results':
            data['version'] = '1'

        if isinstance(message, str) and args:
            data['msg'] = message % args
        # n00454707 2018-08-22 change start
        elif isinstance(message, BaseException):
            import traceback
            data['msg'] = traceback.format_exc()
        # n00454707 2018-08-22 change end
        else:
            data['msg'] = message

        # Set width to a really large value in order to always get one line.
        # But keep this reasonable because the logs will be loaded by CLoader
        # that is limited to around 10**7 chars
        try:
            data_str = json.dumps(data, ensure_ascii=False)
        except TypeError:
            data["msg"] = str(message)
            data_str = json.dumps(data, ensure_ascii=False)
        # data_str = yaml.dump(data, default_flow_style=True,
        #                      default_style='"',
        #                      width=10 ** 6,
        #                      Dumper=yaml.CDumper)[:-1]
        # Test the limit and skip if the line is too long
        if len(data_str) >= 10 ** 6:
            if isinstance(message, str):
                data['msg'] = "<line way too long ...>"
            else:
                data['msg'] = {"skip": "line way too long ..."}
            # data_str = yaml.dump(data, default_flow_style=True,
            #                      default_style='"',
            #                      width=10 ** 6,
            #                      Dumper=yaml.CDumper)[:-1]
            data_str = json.dumps(data)
        self._log(level, data_str, ())
        if level_name == 'results':
            self.handler.send_result(data_str)

    def exception(self, exc, *args, **kwargs):
        self.log_message(logging.ERROR, 'exception', exc, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        self.log_message(logging.ERROR, 'error', message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        self.log_message(logging.WARNING, 'warning', message, *args, **kwargs)

    warn = warning

    def info(self, message, *args, **kwargs):
        self.log_message(logging.INFO, 'info', message, *args, **kwargs)

    def debug(self, message, *args, **kwargs):
        self.log_message(logging.DEBUG, 'debug', message, *args, **kwargs)

    def input(self, message, *args, **kwargs):
        self.log_message(logging.INFO, 'input', message, *args, **kwargs)

    def target(self, message, *args, **kwargs):
        self.log_message(logging.INFO, 'target', message, *args, **kwargs)

    def feedback(self, message, *args, **kwargs):
        self.log_message(logging.INFO, 'feedback', message, *args, **kwargs)

    def results(self, results, *args, **kwargs):
        if 'extra' in results and 'level' not in results:
            raise Exception("'level' is mandatory when 'extra' is used")
        self.log_message(logging.INFO, 'results', results, *args, **kwargs)

         
def setup_logger(options):
    # Pipeline always log as YAML so change the base logger.
    # Every calls to logging.getLogger will now return a YAMLLogger
    logging.setLoggerClass(YAMLLogger)

    # The logger can be used by the parser and the Job object in all phases.
    logger = logging.getLogger('dispatcher')
    if options.logging_url is not None:
        if options.master_cert and options.slave_cert:
            if not os.path.exists(options.master_cert) or not os.path.exists(options.slave_cert):
                return None
        # pylint: disable=no-member
        logger.addZMQHandler(options.logging_url,
                             options.master_cert,
                             options.slave_cert,
                             options.job_id,
                             options.ipv6)
    else:
        logger.addHandler(logging.StreamHandler())

    return logger
            

    
# coding=utf-8
"""
jenkins job submit belong to Hava2
"""
import collections
import copy
import sys
import time

from action_base import ActionBase
from gcov_action import GcovAction
from self_exception import SelfException
from common_util import CommonUtil
from file_util import history_file_dir
from smoke_action import SmokeAction
from str_util import format_str
from const import *
from hdfs_util import *
import re

START_TIME = time.time()
global SYS_ARGVS
def get_params():
    """
     解析脚本传入参数,筛选含有#的行，返回dict
    """
    result = collections.OrderedDict()
    role_num = 1
    device_group = {}
    exclude_keys = ['common_jenkins', 'common_hava']
    params = [x for x in sys.argv if "#" in x]
    rem = '$'
    if len(params) <= 0:
        raise SelfException('no argument')
    for item in params:
        name, value = item.split('#')
        temp = {}
        for element in value.split(';'):
            # device_group类型特殊处理
            if name == 'device_group':
                device = {}
                parse_device_group(element, device)
                device_group["role-"+str(role_num)] = device
                role_num += 1
            else:
                key = element.split('=')[0]
                value = format_str(element.split('=')[1])
                # format_str处理完['aaa.rar', 'bbb.rar']为list类型，处理完[aaa.rar, bbb.rar]为str类型
                if "image_list" in key:  # 处理image_list为str类型问题
                    if type(value) == list:
                        temp[key] = value
                    else:
                        try:
                            temp[key] = [x for x in value[1:-1].split(',')]  # 截取分割列表化
                        except Exception as e:
                            temp[key] = [x for x in value[key]]
                else:
                    temp[key] = value

        if result.has_key(name):
            if name in exclude_keys:
                result[name].update(temp)
            elif name == 'device_group':
                pass
            else:
                # 对于重复action先加$避免key重复
                result[name + rem] = temp
                rem += '$'
        else:
            result[name] = temp
    result['device_group'] = device_group

    return result


def parse_device_group(element, device):
    """
    解析device_group,取消对exe_platform和env_type的解析
    """
    if len(element.split(',')) == 1:
        device['count'] = int(element.split('=')[1])
        device['device_type'] = element.split('=')[0]
    else:
        raise SelfException(
            "Param config error")


def validate_params():
    """
    校验入参
    """
    if SYS_ARGVS.has_key('common_jenkins'):
        jenkins_argvs = SYS_ARGVS['common_jenkins']
        if jenkins_argvs.has_key('jenkins_name'):
            print 'history file dir %s' % history_file_dir(jenkins_argvs['jenkins_name'])
        else:
            raise SelfException('jenkins_name must be specified')
        if not jenkins_argvs.has_key("image_from"):
            raise SelfException('no image from info')
        if jenkins_argvs['image_from'] in ['hdfs', 'mount']:
            if not (jenkins_argvs.has_key('image_url') or jenkins_argvs.has_key('smoke_image_url') or
                    jenkins_argvs.has_key('pre_image_url')):
                raise SelfException('no image url info')
        else:
            raise SelfException("unknown image from type: %s" % jenkins_argvs["image_from"])
        if jenkins_argvs.has_key("strategy"):
            if jenkins_argvs['strategy'] not in [STRATEGY_SINGLE_LATEST, STRATEGY_LWSMOKE, STRATEGY_TEAM,
                                                 STRATEGY_APSMOKE]:
                raise SelfException('%s is an unsupported strategy' % jenkins_argvs['strategy'])
            elif jenkins_argvs['strategy'] == STRATEGY_TEAM and not jenkins_argvs.has_key('team_name'):
                raise SelfException('you should specify the team name at the same time')


def parse_params():
    """
    解析入参，初始化全局变量
    """
    jenkins_argvs = SYS_ARGVS['common_jenkins']
    gen_dict = {'submit_times': 1, 'max_time': 1, 'history_save_days': -1, 'reverse': 'false', 'history_type': 'file', 'duration': 5,
                'stress_times': 1, 'submit_num': 1, 'max_num': -1, 'team_name': '', 'match_repo': False}
    if jenkins_argvs.has_key('stress_times'):
        stress_times = jenkins_argvs['stress_times']
        if stress_times > 200:
            print("stress_times is too big!")
            gen_dict['stress_times'] = 1
    if jenkins_argvs.has_key('strategy') and jenkins_argvs['strategy'] == STRATEGY_LWSMOKE:
        gen_dict['submit_times'] = 300
        gen_dict['max_time'] = 57
    if jenkins_argvs.has_key('submit_times'):
        gen_dict['submit_times'] = jenkins_argvs['submit_times']
    if jenkins_argvs.has_key('max_time'):
        gen_dict['max_time'] = jenkins_argvs['max_time']
    if jenkins_argvs.has_key('image_list'):
        gen_dict['image_list'] = jenkins_argvs['image_list']
    if jenkins_argvs.has_key("template"):
        if jenkins_argvs['template'] == 'none':
            gen_dict['template_name'] = None
        else:
            gen_dict['template_name'] = jenkins_argvs['template'].split('.')[0] + '.yaml'
    else:
        gen_dict['template_name'] = jenkins_argvs['jenkins_name'] + '_template.json'
    gen_dict['history_type'] = 'file'
    if jenkins_argvs.has_key("strategy"):
        if jenkins_argvs['strategy'] == STRATEGY_SINGLE_LATEST:
            gen_dict['duration'] = 5
            gen_dict['max_num'] = 1
            gen_dict['submit_num'] = 1
        elif jenkins_argvs['strategy'] == STRATEGY_LWSMOKE:
            gen_dict['max_num'] = 30
            gen_dict['submit_num'] = 30
            gen_dict['reverse'] = 'true'
            gen_dict['duration'] = 2
        elif jenkins_argvs['strategy'] == STRATEGY_APSMOKE:
            gen_dict['duration'] = 2
            gen_dict['max_num'] = -1
            gen_dict['submit_num'] = 1
            gen_dict['reverse'] = 'true'
        elif jenkins_argvs['strategy'] == STRATEGY_TEAM:
            gen_dict['history_type'] = 'database'
            gen_dict['duration'] = 1
            gen_dict['max_num'] = -1
            gen_dict['submit_num'] = 1
    if jenkins_argvs.has_key("duration"):
        gen_dict['duration'] = jenkins_argvs['duration']
    if gen_dict.has_key('history_type') and jenkins_argvs.has_key('history_type'):
        gen_dict['history_type'] = jenkins_argvs['history_type']
        if gen_dict['history_type'] not in ['file', 'database', 'none']:
            raise SelfException('currently, history type should be one of file database and none')
    if not gen_dict.has_key('duration') and jenkins_argvs.has_key("duration"):
        gen_dict['duration'] = int(jenkins_argvs['duration'])
    if not gen_dict.has_key('submit_num') and jenkins_argvs.has_key("submit_num"):
        gen_dict['submit_num'] = int(jenkins_argvs['submit_num'])
    if not gen_dict.has_key('reverse') and jenkins_argvs.has_key("reverse"):
        gen_dict['reverse'] = jenkins_argvs['reverse'].lower()
    if not gen_dict.has_key('max_num') and jenkins_argvs.has_key("max_num"):
        gen_dict['max_num'] = int(jenkins_argvs['max_num'])
    if jenkins_argvs.has_key("history_save_days"):
        if gen_dict['history_type'] != 'file':
            print 'history_save_days only works for file history type'
        gen_dict['history_save_days'] = int(jenkins_argvs['history_save_days'])

    if jenkins_argvs.has_key("team_name"):
        gen_dict['team_name'] = jenkins_argvs['team_name']
        if gen_dict['history_type'] != 'database':
            raise SelfException('currently, we only support to search image by team name with database')
    if jenkins_argvs.has_key('match_repo') and jenkins_argvs['match_repo'].lower() == 'true':
        gen_dict['match_repo'] = True
    return gen_dict


def strip_space(argvs_dict):
    '''
    delete jenkin configs space
    '''
    if not isinstance(argvs_dict, dict):
        print 'error strip_space!!!'
        return
    new_dict = collections.OrderedDict()
    for key, val in argvs_dict.iteritems():
        if isinstance(val, dict):
            new_dict[key.strip()] = strip_space(val)
        elif isinstance(val, str):
            new_dict[key.strip()] = val.strip()
        else:
            new_dict[key.strip()] = val
    return new_dict


def process_actions(common_init, target_path_list, timestamp_list, image_param_list):
    p_list = []
    gcov_action = GcovAction(common_init)
    gcov_action.set_image_url_list(target_path_list)
    p_list.append(gcov_action)

    smoke_action = SmokeAction(common_init)
    smoke_action.set_params(target_path_list, timestamp_list, image_param_list)
    p_list.append(smoke_action)
    for p in p_list:
        if isinstance(p, ActionBase):
            p.modify_action()


def common_init_process(sys_args, gen_dict):
    flag = False
    try:
        print sys_args
        print '---------------------------------'
        print gen_dict
        common_init = CommonUtil(sys_args, gen_dict)
        common_init.initial()

        # 变量初始化
        target_path_list, timestamp_list, image_param_list, changed_func_dict_list = [], [], [], []

        # Jenkins构建参数中配置image_url=''，则不进行(查找版本)serarch_image_list及get_changed_func_dict_list相关操作
        # Jenkins构建参数中不带有image_url(参数为smoke_image_url, pre_image_ur等其他形式)，
        # 则get('iamge_url')为None，None != '', 继续进行查找版本(走原逻辑)
        if judgement_of_search_image(common_init):  # 用来判断是否需要扫描版本
            target_path_list, timestamp_list, image_param_list = common_init.search_image_list()
            changed_func_dict_list = common_init.get_changed_func_dict_list(target_path_list)

        # common_init.yaml_obj属性的初始化，即提交到hava的job的definition初始化
        common_init.generate_yaml()

        # Jenkins构建参数中配置image_url=''，则不进行process_actions操作(同上文)
        if judgement_of_search_image(common_init):
            process_actions(common_init, target_path_list, timestamp_list, image_param_list)

        # 提交job(若未查找版本，submit_hava_job传入的参数皆为[]，空列表)
        flag = common_init.submit_hava_job(target_path_list, timestamp_list, image_param_list, changed_func_dict_list)
    except SelfException, err:
        print "common_init_process error:%s" % err
    return flag


# 版本扫描条件过滤器(类似于v1的init_data方法中扫描版本的判断)
def judgement_of_search_image(common_init):

    # 李晶晶需求，若image_url不为空，断定：扫描版本
    condition_first = (common_init.jenkins_argvs.get('image_url') != '')

    # 孙书显需求，(有image_list，并且有image_url, 且image_url里面没有@号)此时不需要扫描版本，对整体进行not('非')操作，断定：扫描版本
    condition_second = not (common_init.jenkins_argvs.has_key('image_list')
                            and common_init.jenkins_argvs.has_key('image_url')
                            and '@' not in common_init.jenkins_argvs.get('image_url'))

    return condition_first and condition_second  # 一非皆非，若需要扫描版本，则所有的断定都需要为True


def lz4_adapter(sys_args, gen_dic):
    args_lz4copy = copy.deepcopy(sys_args)
    gen_dic_copy = copy.deepcopy(gen_dic)
    jenkins_argvs = args_lz4copy['common_jenkins']
    transfer_lz4(jenkins_argvs)
    transfer_lz4(gen_dic_copy)
    return args_lz4copy, gen_dic_copy


def transfer_lz4(dic):
    try:
        for key, value in dic.iteritems():
            if "_url" in key or "package" in key:
                dic[key] = replace_str(value)
            elif "image_list" in key:
                try:
                    dic[key] = [replace_str(x) for x in dic[key][1:-1].split(',')]
                except Exception as e:
                    # 用户输入image_list=[aaa.tar.lz4,bbb.tar.lz4], 若列表类型,直接进行处理
                    dic[key] = [replace_str(x) for x in dic[key]]
    except SelfException, err:
        print "transfer_lz4 error:%s" % err


def replace_str(str):
    # 若str本身就是.tar.lz4格式，则不用替换
    return str if '.tar.lz4' in str else str.replace('.rar', '.tar.lz4')
    # return str.replace('.rar', '.tar.lz4')


def judgement_of_whether_lz4_transform(param):
    # 有image_url且image_url里面没有@号，则不需要替换lz4包，此种情况是为了区分image_url情况下扫描多包
    return param['common_jenkins'].has_key('image_url') \
           and param['common_jenkins'].has_key('image_list') \
           and '@' not in param['common_jenkins'].get('image_url')


def adapter(sys_args, gen_dic):  # 非rar转换深拷贝
    args_copy = copy.deepcopy(sys_args)
    gen_dic_copy = copy.deepcopy(gen_dic)
    return args_copy, gen_dic_copy


def scan_single_outter_path(img_str, upper_layer_path, wildcard_path):  # 扫描外层一级路径

    ret, path_list = hdfs.list_path(upper_layer_path)  # 拿到hdfs层级

    target_path_list = []

    for path in path_list:  # 匹配所有符合条件的版本
        target_path = re.findall(wildcard_path, str(path))
        if target_path and len(wildcard_path) == len(str(path)):
            target_path_list.append(target_path[0])

    if target_path_list:
        target_path_list.sort(reverse=True)  # 逆序取最新版本
        return target_path_list
    else:
        return target_path_list


def scan_multi_inner_path(img_str, pkg_str, arg1):  # 扫描内层多级路径
    target_path_list = []
    # 每一层都要循环到，保证不能漏扫任何一个包
    # 最外层/compilepackage/CI_Version/hihms/br_hisi_wt_trunk_accekit_PRE_COMPILE
    first_path_list = traverse_hdfs_path(img_str, 'kirin', 1)
    for first_path in first_path_list:

        # 第二层/compilepackage/CI_Version/hihms/br_hisi_wt_trunk_accekit_PRE_COMPILE/202006/
        second_path_list = traverse_hdfs_path(first_path, 'kirin', 1)  # 第二层
        for second_path in second_path_list:

            # 第三层/compilepackage/CI_Version/hihms/br_hisi_wt_trunk_accekit_PRE_COMPILE/202006/20200615_101434323_I1eca3c2
            ret, third_path_list = hdfs.list_path(second_path)
            for third_path in third_path_list:

                if arg1['common_jenkins'].has_key('image_list'):
                    pkg_list = arg1['common_jenkins']['image_list']
                    for pkg in pkg_list:
                        target_path = re.findall(pkg.replace('*', '.'), str(third_path))
                        if target_path and len(pkg) == len(str(third_path)):
                            target_path_list.append(target_path[0])
                    if len(target_path_list) == len(pkg_list) :  # 保证多个包都扫到
                        return target_path_list
                else:
                    target_path = re.findall(pkg_str.replace('*', '.'), str(third_path))
                    if target_path and len(pkg_str) == len(str(third_path)):
                        target_path_list.append(target_path[0])

                    if len(target_path_list) > 0:
                        return target_path_list
    return target_path_list


def check_hdfs_image_url(param, arg1, arg2):

    img_pkg_list = param.split('@')  # 拿到路径名和包名

    img_str = img_pkg_list[0]  # 路径名: '/compilepackage/CI_Version/baltimore/br_release_baltimorev100r001c10b***'
    pkg_str = img_pkg_list[1]  # 包名: 'baltimore_***.rar'

    if '*' in img_str:  # 如果通配符出现在路径中
        upper_layer_path = img_str.rsplit('/', 1)[0]  # 切割出上层路径
        wildcard_path = img_str.rsplit('/', 1)[1].replace('*', '.')  # 切割出通配符路径，并将通配符路径转换为正则表达式

        target_path_list = scan_single_outter_path(img_str, upper_layer_path, wildcard_path)  # 具体处理函数

        if target_path_list:
            # 拼接并更改全局变量里image_url，将其替换为最新版本路径
            arg1['common_jenkins']['image_url'] = upper_layer_path + '/' + target_path_list[0] + '@' + pkg_str

    else:  # 如果通配符出现在包名中，情况很复杂，在后面具体扫描版本时处理
        print img_str
        print pkg_str
        target_path_list = scan_multi_inner_path(img_str, pkg_str, arg1)
        if target_path_list:
            arg1['common_jenkins']['image_url'] = img_str + '@' + target_path_list[0]
            if arg1['common_jenkins'].has_key('image_list'):
                arg1['common_jenkins']['image_list'] = target_path_list
            if arg2.has_key('image_list'):
                arg2['image_list'] = target_path_list

        print 'hello wolrd'


if __name__ == '__main__':
    try:
        global SYS_ARGVS
        # 解析入参
        SYS_ARGVS = strip_space(get_params())
        # 验证入参
        validate_params()
        # 入参初始化
        gen_dict = parse_params()

        # 校验hdfs的image_url是否有通配符，如果有，则进行通配符问题处理
        if '*' in SYS_ARGVS['common_jenkins'].get('image_url', ''):
            check_hdfs_image_url(SYS_ARGVS['common_jenkins'].get('image_url'), SYS_ARGVS, gen_dict)

        # 判断是否进行lz4后缀替换
        if judgement_of_whether_lz4_transform(SYS_ARGVS):
            sys_args_lz4copy, gen_dict_lz4copy = adapter(SYS_ARGVS, gen_dict)
        else:
            sys_args_lz4copy, gen_dict_lz4copy = lz4_adapter(SYS_ARGVS, gen_dict)

        flag = common_init_process(sys_args_lz4copy, gen_dict_lz4copy)
        if not flag:
            common_init_process(SYS_ARGVS, gen_dict)
    except SelfException, err:
        print "parse params error,exit"
        sys.exit()
    print 'max_time: ', gen_dict['max_time'], ' submit_times: ', gen_dict['submit_times']
    for i in range(gen_dict['submit_times']):
        print 'execute times: %d' % (i + 1)
        time.sleep(0.1)
        total_time = time.time() - START_TIME
        print 'total time: %fs\n\n' % total_time
        if total_time > gen_dict['max_time']:
            print 'reach the max_time: %f' % gen_dict['max_time']
            break

