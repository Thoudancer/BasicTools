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
import portalocker.constants as porta_lock_const
from portalocker.utils import Lock as PortaLock
# import watchtower


class ComposeRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, maxBytes=100, backupCount=0, encoding=None, delay=False,
                 utc=False, atTime=None):
        TimedRotatingFileHandler.__init__(self, filename, when, interval, backupCount, encoding, delay, utc, atTime)
        # new config
        file_path = os.path.split(filename)[0]
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        self.day = 0
        self.cnt = 0
        self.maxBytes = maxBytes

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        record is not used, as we are just comparing times, but it is needed so
        the method signatures are the same
        """
        t = int(time.time())
        # t = int(record.created)
        if t >= self.rolloverAt:
            SafeComposeRotatingFileHandler.before_rollover_cnt = self.cnt  # 记录当前cnt
            self.day = 1  # 当前周期翻转，通知计算下次周期
            return 1
        else:

            if self.stream is None:  # delay was set...
                self.stream = self._open()
            if self.maxBytes > 0:  # are we rolling over?
                msg = "%s\n" % self.format(record)
                self.stream.seek(0, 2)  # due to non-posix-compliant Windows feature
                if self.stream.tell() + len(msg) >= self.maxBytes:
                    self.day = 0  # 周期保持
                    return 1

            return 0

    def doRollover(self):
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        self.cnt += 1  # 本周期 cnt+1
        dfn = self.rotation_filename(
            self.baseFilename[:-4] + "_" + time.strftime(self.suffix, timeTuple) + '_{}.log'.format(
                self.cnt))
        SafeComposeRotatingFileHandler.before_rollover_cnt = self.cnt  # 记录当前cnt
        SafeComposeRotatingFileHandler.before_rollover_at = currentTime
        if os.path.exists(dfn):
            # 因为进程变量不会在内存同步，所以存在其他进程已经翻转过日志文件当时当前进程中还标识为未翻转
            # 日志内容创建时间如果小于等于下一个处理翻转时刻，则将日志写入反转后的日志文件，而不是当前的baseFilename
            # 当前磁盘上的baseFilename对于当前进程中的标识副本来说已经是翻转后要写入的文件
            # 所以当文件存在时，本次不再进行翻转动作
            pass
        else:
            self.rotate(self.baseFilename, dfn)

        # delete old file
        self.getFilesToDelete()
        if not self.delay:
            self.stream = self._open()
        if self.day == 1:
            self.cnt = 0  # 新周期cnt 置0
            newRolloverAt = self.computeRollover(currentTime)
            while newRolloverAt <= currentTime:
                newRolloverAt = newRolloverAt + self.interval
            # If DST changes and midnight or weekly rollover, adjust for this.
            if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
                dstAtRollover = time.localtime(newRolloverAt)[-1]
                if dstNow != dstAtRollover:
                    if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                        addend = -3600
                    else:  # DST bows out before next rollover, so we need to add an hour
                        addend = 3600
                    newRolloverAt += addend
            # 此刻，当前进程中的标识副本已经同步为最新
            self.rolloverAt = newRolloverAt
            self.day = 0

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.

        More specific than the earlier method, which just used glob.glob().
        """
        if self.backupCount <= 0:
            return
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        timeSize = set()
        prefix = baseName[:-4] + "_"  # xx.log-->xx_2022-12-12_10-20-20_1.log
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                sizeN = len(suffix.split('_')[-1])
                suffix = suffix[:-sizeN - 1]
                if self.extMatch.match(suffix):
                    timeSize.add(suffix)
                    result.append(os.path.join(dirName, fileName))
        if len(timeSize) < self.backupCount:
            resultDel = []
        else:
            timeSize = list(timeSize)
            timeSize.sort()
            timeSize = timeSize[:len(timeSize) - self.backupCount]
            resultDel = [file for file in result if any(ti in file for ti in timeSize)]
        for file in resultDel:
            try:
                os.remove(file)
            except:
                pass
        return resultDel


class ConcurrentLogFileLock(PortaLock):
    def __init__(self, filename, *args, **kwargs):
        PortaLock.__init__(self, self.get_lock_filename(filename), *args, **kwargs)

    def get_lock_filename(self, log_file_name):
        """
        定义日志文件锁名称，类似于 `.__file.lock`，其中file与日志文件baseFilename一致
        :return: 锁文件名称
        """
        if log_file_name.endswith(".log"):
            lock_file = log_file_name[:-4]
        else:
            lock_file = log_file_name
        lock_file += ".lock"
        lock_path, lock_name = os.path.split(lock_file)
        # hide the file on Unix and generally from file completion
        lock_name = ".__" + lock_name
        return os.path.join(lock_path, lock_name)


class SafeComposeRotatingFileHandler(TimedRotatingFileHandler):
    """
    Handler for logging to a file, rotating the log file at certain timed
    intervals.

    If backupCount is > 0, when rollover is done, no more than backupCount
    files are kept - the oldest ones are deleted.
    """

    before_rollover_at = -1  # 上一次翻转时间
    before_rollover_cnt = 0  # 上个cnt

    def __init__(self, filename, when='h', interval=1, maxBytes=100, backupCount=0, encoding=None, delay=False,
                 utc=False, atTime=None):
        TimedRotatingFileHandler.__init__(self, filename, when, interval, backupCount, encoding, delay, utc, atTime)
        # new config
        file_path = os.path.split(filename)[0]
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        self.day = 0
        self.cnt = 0
        self.maxBytes = maxBytes
        self.concurrent_lock = ConcurrentLogFileLock(filename, flags=porta_lock_const.LOCK_EX)

    def emit(self, record) -> None:
        """
        本方法继承Python标准库,修改的部分已在下方使用注释标记出
        本次改动主要是对日志文件进行加锁，并且保证在多进程环境下日志内容切割正确
        """
        # 此行为新增代码，尝试获取非重入进程锁，阻塞，直到成功获取
        with self.concurrent_lock:
            try:
                if self.shouldRollover(record):
                    self.doRollover()

                """
                如果日志内容创建时间小于上一次翻转时间，不能记录在baseFilename文件中，否则正常记录

                处理日志写入哪个日志文件，修改开始
                """

                if record.created <= SafeComposeRotatingFileHandler.before_rollover_at:
                    # v 引用Python3.7标准库logging.TimedRotatingFileHandler.doRollover(110:124)中翻转目标文件名生成代码 v
                    currentTime = int(record.created)
                    dstNow = time.localtime(currentTime)[-1]
                    # current time tuple
                    t = self.computeRollover(currentTime) - self.interval
                    if self.utc:
                        timeTuple = time.gmtime(t)
                    else:
                        timeTuple = time.localtime(t)
                        dstThen = timeTuple[-1]
                        if dstNow != dstThen:
                            if dstNow:
                                addend = 3600
                            else:
                                addend = -3600
                            timeTuple = time.localtime(t + addend)
                    dfn = self.rotation_filename(
                        self.baseFilename[:-4] + "_" + time.strftime(self.suffix, timeTuple) + '_{}.log'.format(
                            SafeComposeRotatingFileHandler.before_rollover_cnt))
                    # ^ 引用标准库TimedRotatingFileHandler中翻转目标文件名生成规则代码                                  ^

                    # 如果back_count值设置的过低，会出现日志文件实际数量大于设置值
                    # 因为当日志写入负载过高时，之前的某个时刻产生的日志会延迟到现在才进行写入，在写入时又找不到与时间对应的日志文件，
                    # 则会再创建一个与日志创建时刻对应的日志文件进行写入。
                    # 对应的日志文件是指达到翻转条件后创建的翻转文件，文件命名规则与标准库一致。
                    self._do_write_record(dfn, record)
                else:
                    logging.FileHandler.emit(self, record)
                """
                处理日志写入哪个日志文件，修改结束
                """
            except Exception:
                self.handleError(record)

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        record is not used, as we are just comparing times, but it is needed so
        the method signatures are the same
        """
        t = int(time.time())
        # t = int(record.created)
        if t >= self.rolloverAt:
            SafeComposeRotatingFileHandler.before_rollover_cnt = self.cnt  # 记录当前cnt
            self.day = 1  # 当前周期翻转，通知计算下次周期
            return 1
        else:

            if self.stream is None:  # delay was set...
                self.stream = self._open()
            if self.maxBytes > 0:  # are we rolling over?
                msg = "%s\n" % self.format(record)
                self.stream.seek(0, 2)  # due to non-posix-compliant Windows feature
                if self.stream.tell() + len(msg) >= self.maxBytes:
                    self.day = 0  # 周期保持
                    return 1

            return 0

    def doRollover(self):
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        self.cnt += 1  # 本周期 cnt+1
        dfn = self.rotation_filename(
            self.baseFilename[:-4] + "_" + time.strftime(self.suffix, timeTuple) + '_{}.log'.format(
                self.cnt))
        SafeComposeRotatingFileHandler.before_rollover_cnt = self.cnt  # 记录当前cnt
        SafeComposeRotatingFileHandler.before_rollover_at = currentTime
        if os.path.exists(dfn):
            # 因为进程变量不会在内存同步，所以存在其他进程已经翻转过日志文件当时当前进程中还标识为未翻转
            # 日志内容创建时间如果小于等于下一个处理翻转时刻，则将日志写入反转后的日志文件，而不是当前的baseFilename
            # 当前磁盘上的baseFilename对于当前进程中的标识副本来说已经是翻转后要写入的文件
            # 所以当文件存在时，本次不再进行翻转动作
            pass
        else:
            self.rotate(self.baseFilename, dfn)

        # delete old file
        self.getFilesToDelete()
        if not self.delay:
            self.stream = self._open()
        if self.day == 1:
            self.cnt = 0  # 新周期cnt 置0
            newRolloverAt = self.computeRollover(currentTime)
            while newRolloverAt <= currentTime:
                newRolloverAt = newRolloverAt + self.interval
            # If DST changes and midnight or weekly rollover, adjust for this.
            if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
                dstAtRollover = time.localtime(newRolloverAt)[-1]
                if dstNow != dstAtRollover:
                    if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                        addend = -3600
                    else:  # DST bows out before next rollover, so we need to add an hour
                        addend = 3600
                    newRolloverAt += addend
            # 此刻，当前进程中的标识副本已经同步为最新
            self.rolloverAt = newRolloverAt
            self.day = 0

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.

        More specific than the earlier method, which just used glob.glob().
        """
        if self.backupCount <= 0:
            return
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        timeSize = set()
        prefix = baseName[:-4] + "_"  # xx.log-->xx_2022-12-12_10-20-20_1.log
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                sizeN = len(suffix.split('_')[-1])
                suffix = suffix[:-sizeN - 1]
                if self.extMatch.match(suffix):
                    timeSize.add(suffix)
                    result.append(os.path.join(dirName, fileName))
        if len(timeSize) < self.backupCount:
            resultDel = []
        else:
            timeSize = list(timeSize)
            timeSize.sort()
            timeSize = timeSize[:len(timeSize) - self.backupCount]
            resultDel = [file for file in result if any(ti in file for ti in timeSize)]
        for file in resultDel:
            try:
                os.remove(file)
            except:
                pass
        return resultDel

    def _do_write_record(self, dfn, record):
        """
        将日志内容写入指定文件
        :param dfn: 指定日志文件
        :param record: 日志内容
        """
        with open(dfn, mode="a", encoding=self.encoding) as file:
            file.write(self.format(record) + self.terminator)


class SafeTimeRotatingFileHandler(TimedRotatingFileHandler):
    # 上一次翻转时间
    before_rollover_at = -1

    def __init__(self, filename, *args, **kwargs):
        TimedRotatingFileHandler.__init__(self, filename, *args, **kwargs)

        file_path = os.path.split(filename)[0]
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        self.concurrent_lock = ConcurrentLogFileLock(filename, flags=porta_lock_const.LOCK_EX)

    def emit(self, record) -> None:
        """
        本方法继承Python标准库,修改的部分已在下方使用注释标记出
        本次改动主要是对日志文件进行加锁，并且保证在多进程环境下日志内容切割正确
        """
        # 此行为新增代码，尝试获取非重入进程锁，阻塞，直到成功获取
        with self.concurrent_lock:
            try:
                if self.shouldRollover(record):
                    self.doRollover()

                """
                如果日志内容创建时间小于上一次翻转时间，不能记录在baseFilename文件中，否则正常记录

                处理日志写入哪个日志文件，修改开始
                """
                if record.created <= SafeTimeRotatingFileHandler.before_rollover_at:
                    currentTime = int(record.created)
                    # v 引用Python3.7标准库logging.TimedRotatingFileHandler.doRollover(110:124)中翻转目标文件名生成代码 v
                    dstNow = time.localtime(currentTime)[-1]
                    t = self.computeRollover(currentTime) - self.interval
                    if self.utc:
                        timeTuple = time.gmtime(t)
                    else:
                        timeTuple = time.localtime(t)
                        dstThen = timeTuple[-1]
                        if dstNow != dstThen:
                            if dstNow:
                                addend = 3600
                            else:
                                addend = -3600
                            timeTuple = time.localtime(t + addend)
                    dfn = self.rotation_filename(self.baseFilename + "." +
                                                 time.strftime(self.suffix, timeTuple))
                    # ^ 引用标准库TimedRotatingFileHandler中翻转目标文件名生成规则代码                                  ^

                    # 如果back_count值设置的过低，会出现日志文件实际数量大于设置值
                    # 因为当日志写入负载过高时，之前的某个时刻产生的日志会延迟到现在才进行写入，在写入时又找不到与时间对应的日志文件，
                    # 则会再创建一个与日志创建时刻对应的日志文件进行写入。
                    # 对应的日志文件是指达到翻转条件后创建的翻转文件，文件命名规则与标准库一致。
                    self._do_write_record(dfn, record)
                else:
                    logging.FileHandler.emit(self, record)
                """
                处理日志写入哪个日志文件，修改结束
                """
            except Exception:
                self.handleError(record)

    def doRollover(self):
        """
        本方法继承Python标准库,修改的部分已在下方使用注释标记出
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        dfn = self.rotation_filename(self.baseFilename + "." +
                                     time.strftime(self.suffix, timeTuple))
        """
        如果翻转文件已经生成，则说明其他进程已经处理过翻转
        处理日志文件已经翻转当前进程中未写入文件的日志副本，修改开始
        """
        # 直接修改静态变量，因为代码执行到此处已经获取到非重入进程锁，保证同一时间只有一个线程对变量进行修改
        # 由于Python GIL，同一时间同一进程内只有一个线程运行，线程切换后缓存自动失效，即其他线程可以看见修改后的最新值
        # 记录每一次触发翻转动作的时间，不管反转是否真的执行
        SafeTimeRotatingFileHandler.before_rollover_at = self.rolloverAt
        if os.path.exists(dfn):
            # 因为进程变量不会在内存同步，所以存在其他进程已经翻转过日志文件当时当前进程中还标识为未翻转
            # 日志内容创建时间如果小于等于下一个处理翻转时刻，则将日志写入反转后的日志文件，而不是当前的baseFilename
            # 当前磁盘上的baseFilename对于当前进程中的标识副本来说已经是翻转后要写入的文件
            # 所以当文件存在时，本次不再进行翻转动作
            pass
        else:
            self.rotate(self.baseFilename, dfn)
        """
        处理日志文件已经翻转当前进程中未写入文件的日志副本，修改结束
        """
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        if not self.delay:
            self.stream = self._open()
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                    addend = -3600
                else:  # DST bows out before next rollover, so we need to add an hour
                    addend = 3600
                newRolloverAt += addend
        # 此刻，当前进程中的标识副本已经同步为最新
        self.rolloverAt = newRolloverAt

    def _do_write_record(self, dfn, record):
        """
        将日志内容写入指定文件
        :param dfn: 指定日志文件
        :param record: 日志内容
        """
        with open(dfn, mode="a", encoding=self.encoding) as file:
            file.write(self.format(record) + self.terminator)


class SafeRotatingFileHandler(RotatingFileHandler):
    # 上一次翻转时间
    before_rollover_at = -1

    def __init__(self, filename, *args, **kwargs):
        RotatingFileHandler.__init__(self, filename, *args, **kwargs)

        file_path = os.path.split(filename)[0]
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        self.cnt = 0

        self.concurrent_lock = ConcurrentLogFileLock(filename, flags=porta_lock_const.LOCK_EX)

    def emit(self, record) -> None:
        """
        本方法继承Python标准库,修改的部分已在下方使用注释标记出
        本次改动主要是对日志文件进行加锁，并且保证在多进程环境下日志内容切割正确
        """
        # 此行为新增代码，尝试获取非重入进程锁，阻塞，直到成功获取
        with self.concurrent_lock:
            try:
                if self.shouldRollover(record):
                    self.doRollover()

                """
                如果日志内容创建时间小于上一次翻转时间，不能记录在baseFilename文件中，否则正常记录

                处理日志写入哪个日志文件，修改开始
                """
                if record.created <= SafeRotatingFileHandler.before_rollover_at:
                    dfn = self.rotation_filename("%s" % self.baseFilename)
                    # ^ 引用标准库RotatingFileHandler中翻转目标文件名生成规则代码                                  ^

                    # 如果back_count值设置的过低，会出现日志文件实际数量大于设置值
                    # 因为当日志写入负载过高时，之前的某个时刻产生的日志会延迟到现在才进行写入，在写入时又找不到与时间对应的日志文件，
                    # 则会再创建一个与日志创建时刻对应的日志文件进行写入。
                    # 对应的日志文件是指达到翻转条件后创建的翻转文件，文件命名规则与标准库一致。
                    self._do_write_record(dfn, record)
                else:
                    logging.FileHandler.emit(self, record)
                """
                处理日志写入哪个日志文件，修改结束
                """
            except Exception:
                self.handleError(record)

    def doRollover(self):
        """
        Do a rollover, as described in __init__().
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        self.cnt += 1  # 本周期 cnt+1
        dfn = self.rotation_filename("%s.%d" % (self.baseFilename, self.cnt))
        SafeRotatingFileHandler.before_rollover_at = int(time.time())
        if os.path.exists(dfn):
            # 因为进程变量不会在内存同步，所以存在其他进程已经翻转过日志文件当时当前进程中还标识为未翻转
            # 日志内容创建时间如果小于等于下一个处理翻转时刻，则将日志写入反转后的日志文件，而不是当前的baseFilename
            # 当前磁盘上的baseFilename对于当前进程中的标识副本来说已经是翻转后要写入的文件
            # 所以当文件存在时，本次不再进行翻转动作
            pass
        else:
            self.rotate(self.baseFilename, dfn)

        # delete old file
        if self.backupCount > 0:
            for i in range(1, self.cnt - self.backupCount + 1, 1):
                dfn = self.rotation_filename("{}.{}".format(self.baseFilename, i))
                if os.path.exists(dfn):
                    os.remove(dfn)

        if not self.delay:
            self.stream = self._open()

    def _do_write_record(self, dfn, record):
        """
        将日志内容写入指定文件
        :param dfn: 指定日志文件
        :param record: 日志内容
        """
        with open(dfn, mode="a", encoding=self.encoding) as file:
            file.write(self.format(record) + self.terminator)


class LoggingAsy(threading.Thread):
    """
    log Asynchronous
    example:
    log1 = LoggingAsy("./test.log")
    log1.setDaemon(True)
    log1.start()
    log.info("xxx")
    log1.stop()

    """

    def __init__(self, filename='', interval=1, when='MIDNIGHT', maxBytes="2MB", backupCount=3, method="compose",
                 multiProcess=False, console=True, cloudWatchClient=None, cloudLogGroup="", cloudLogStream="", cloudInterval=5):
        threading.Thread.__init__(self)
        self.aQueue = queue.Queue(100000)
        self.m_running = False
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
        self.logger = logging.getLogger(filename)
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)

        format_str = logging.Formatter(
            fmt='%(asctime)s %(levelname)s %(process)d %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        if not self.logger.hasHandlers():
            if method == "time" and multiProcess:
                th = SafeTimeRotatingFileHandler(filename=filename, interval=interval, when=when,
                                                 backupCount=backupCount, encoding='utf-8')
            elif method == "time" and not multiProcess:
                th = TimedRotatingFileHandler(filename=filename, interval=interval, when=when, backupCount=backupCount,
                                              encoding='utf-8')
            elif method == "size" and multiProcess:
                th = SafeRotatingFileHandler(filename=filename, maxBytes=maxBytes, backupCount=backupCount,
                                             encoding='utf-8')
            elif method == "size" and not multiProcess:
                th = RotatingFileHandler(filename=filename, maxBytes=maxBytes, backupCount=backupCount,
                                         encoding='utf-8')
            elif method == "compose" and multiProcess:
                th = SafeComposeRotatingFileHandler(filename=filename, interval=interval, when=when,
                                                    backupCount=backupCount, maxBytes=maxBytes, encoding='utf-8')
            else:
                th = ComposeRotatingFileHandler(filename=filename, interval=interval, when=when,
                                                backupCount=backupCount, maxBytes=maxBytes, encoding='utf-8')
            th.setFormatter(format_str)
            self.logger.addHandler(th)
            if console:
                ch = logging.StreamHandler()
                ch.setFormatter(format_str)
                self.logger.addHandler(ch)
            if cloudWatchClient is not None:
                import watchtower
                cloudHandler = watchtower.CloudWatchLogHandler(log_group_name=cloudLogGroup,
                                                               log_stream_name=cloudLogStream,
                                                               boto3_client=cloudWatchClient, send_interval=cloudInterval)
                cloudHandler.setFormatter(format_str)
                self.logger.addHandler(cloudHandler)

    def run(self):

        self.m_running = True
        while self.m_running:
            if self.aQueue.empty():
                time.sleep(0.5)
                continue
            data = self.aQueue.get()
            loglevel = data[0]
            content = data[1]
            if 'debug' == loglevel:
                self.logger.debug(*content)
            elif 'info' == loglevel:
                self.logger.info(*content)
            elif 'warning' == loglevel:
                self.logger.warning(*content)
            elif 'error' == loglevel:
                self.logger.error(*content)
            elif 'critical' == loglevel:
                self.logger.critical(*content)

    def stop(self):
        self.m_running = False

    def debug(self, *content):
        self.aQueue.put(('debug', content))

    def info(self, *content):
        self.aQueue.put(('info', content))

    def warning(self, *content):
        self.aQueue.put(('warning', content))

    def error(self, *content):
        self.aQueue.put(('error', content))

    def critical(self, *content):
        self.aQueue.put(('critical', content))


