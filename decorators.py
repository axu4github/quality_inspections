# -*- coding: utf-8 -*-

import time
from functools import wraps


def time_analyze(func):
    """ 装饰器 获取程序执行时间 """
    @wraps(func)
    def consume(*args, **kwargs):
        # 重复执行次数（单次执行速度太快）
        exec_times = 1
        start = time.time()
        for i in range(exec_times):
            r = func(*args, **kwargs)

        finish = time.time()
        print("{:<20}{:10.6} s".format(func.__name__ + ":", finish - start))
        return r

    return consume
