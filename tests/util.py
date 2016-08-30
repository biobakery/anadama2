# -*- coding: utf-8 -*-
import multiprocessing

def timed(func, time, *args, **kwargs):
    p = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
    p.start()
    p.join(time)
    if p.is_alive():
        p.terminate()
        raise Exception("Timed out!")


