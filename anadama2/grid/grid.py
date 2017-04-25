# -*- coding: utf-8 -*-

import threading
import Queue

from .. import runners
    

class GridWorker(threading.Thread):
    """ Base Grid Worker class """
    
    def __init__(self, work_q, result_q, lock, reporter):
        super(GridWorker, self).__init__()
        self.daemon = True
        self.logger = runners.logger
        self.work_q = work_q
        self.result_q = result_q
        self.lock = lock
        self.reporter = reporter    
    
    @staticmethod
    def appropriate_q_class(*args, **kwargs):
        return queue.Queue(*args, **kwargs)    

    @staticmethod
    def appropriate_lock():
        return threading.Lock() 
    
    def run(self):
        raise NotImplementedError