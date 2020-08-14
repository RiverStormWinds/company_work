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
            
            
