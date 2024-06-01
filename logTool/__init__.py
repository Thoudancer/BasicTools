# -*- coding: utf-8 -*-
"""
created on 2021/12/3 15:46
@author:Thoudancer
"""
import os
import time
import logging
import threading
import queue
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from .basicClass import SafeTimeRotatingFileHandler, SafeRotatingFileHandler, SafeComposeRotatingFileHandler, ComposeRotatingFileHandler


def logger(filename='', interval=1, when='MIDNIGHT', maxBytes="2MB", backupCount=3, method="compose", level="info",
           multiProcess=False, console=True, file=True, cloudWatchClient=None, cloudLogGroup="", cloudLogStream="", cloudInterval=5):
    """
    @ Function Description
      log format
    @ Parameters List:
      <str> filename --> log file name, like  test.log
      <int> interval --> log split by time interval
      <str> when -> split time , 'S', 'M', 'H', 'D', 'W', 'MIDNIGHT'
      <str> maxBytes -> log split by file size, Bytes, like KB, MB, GB
      <int> backupCount --> for method time and compose, the days for storing file, for method size, the cnt of file for storing
      <str> method --> {"time", "size", "compose"}
      <bool> mulProcess --> whether support multi process
    """
    assert method in ["time", "size", "compose"]
    if method in ["size", "compose"]:
        maxBytes = maxBytesMapping(maxBytes)

    if filename != '':
        dir_path = os.path.abspath(filename + "/..")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
    else:
        logPath = os.path.abspath(__file__) + '/../log'
        if not os.path.exists(logPath):
            os.makedirs(logPath)
        filename = logPath + '/project.log'
    selfLogger = logging.getLogger(filename)
    selfLogger.propagate = False
    levelMap = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING, "error": logging.ERROR}
    selfLogger.setLevel(levelMap[level])

    format_str = logging.Formatter(fmt='%(asctime)s %(process)d %(funcName)s %(lineno)d %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # logging.Formatter(fmt='%(asctime)s %(process)d %(funcName)s %(lineno)d %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    if not selfLogger.hasHandlers():
        if file:
            if method == "time" and multiProcess:
                th = SafeTimeRotatingFileHandler(filename=filename, interval=interval, when=when, backupCount=backupCount,
                                                 encoding='utf-8')
            elif method == "time" and not multiProcess:
                th = TimedRotatingFileHandler(filename=filename, interval=interval, when=when, backupCount=backupCount,
                                              encoding='utf-8')
            elif method == "size" and multiProcess:
                th = SafeRotatingFileHandler(filename=filename, maxBytes=maxBytes, backupCount=backupCount,
                                             encoding='utf-8')
            elif method == "size" and not multiProcess:
                th = RotatingFileHandler(filename=filename, maxBytes=maxBytes, backupCount=backupCount, encoding='utf-8')
            elif method == "compose" and multiProcess:
                th = SafeComposeRotatingFileHandler(filename=filename, interval=interval, when=when,
                                                    backupCount=backupCount, maxBytes=maxBytes, encoding='utf-8')
            else:
                th = ComposeRotatingFileHandler(filename=filename, interval=interval, when=when, backupCount=backupCount,
                                                maxBytes=maxBytes, encoding='utf-8')
            th.setFormatter(format_str)
            selfLogger.addHandler(th)

        if console:
            ch = logging.StreamHandler()
            ch.setFormatter(format_str)
            selfLogger.addHandler(ch)
        if cloudWatchClient is not None:
            import watchtower
            cloudHandler = watchtower.CloudWatchLogHandler(log_group_name=cloudLogGroup,
                                                           log_stream_name=cloudLogStream,
                                                           boto3_client=cloudWatchClient, send_interval=cloudInterval)
            cloudHandler.setFormatter(format_str)
            selfLogger.addHandler(cloudHandler)
    return selfLogger
def maxBytesMapping(maxBytes):
    if maxBytes[-2:] == "KB":
        maxBytes = int(maxBytes[:-2]) * 1024
    elif maxBytes[-2:] == "MB":
        maxBytes = int(maxBytes[:-2]) * 1024 * 1024
    elif maxBytes[-2:] == "GB":
        maxBytes = int(maxBytes[:-2]) * 1024 * 1024 * 1024
    else:
        maxBytes = 100 * 1024
    return maxBytes


def reportToCloudWatch(logsClient, dataList, log):
    # create log group
    # try:
    #     response1 = logsClient.create_log_group(logGroupName="ttt", kmsKeyId="string1", tags={ 'string': 'string'})
    # except Exception as e:
    #     log.error(e)
    # create log stream
    # response = logsClient.create_log_stream(logGroupName='string',logStreamName='string')

    logGroupName = "iii"
    streamName = "jjj"
    try:
        # describeLogStreamsResponse = logsClient.describe_log_streams(logGroupName=logGroupName, logGroupIdentifier=logGroupName,
        #                                                              logStreamNamePrefix=streamName)
        # sequence_token = describeLogStreamsResponse['logStreams'][0]['uploadSequenceToken']
        # dataProtection = logsClient.put_data_protection_policy(logGroupIdentifier='string', policyDocument='string')
        logEvents = [{'timestamp': int(time.time() * 1000), 'message': m} for m in dataList]
        logsClient.put_log_events(logGroupName=logGroupName, logStreamName=streamName, logEvents=logEvents)
    except Exception as e:
        log.error("cloudwatch log failed", e)
