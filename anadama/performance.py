import re
import os
import json
from collections import namedtuple

import requests

DEFAULT_URL = "./.anadama_performance_history.json"
DEFAULT_MEM = 1024 # 1GB in MB
DEFAULT_TIME = 2*60# 2 hrs in mins
DEFAULT_THREADS = 1

Prediction = namedtuple("Prediction", "mem time threads")

default_prediction = Prediction(DEFAULT_MEM, DEFAULT_TIME, DEFAULT_THREADS)

field_set = set(default_prediction._fields)

# TODO: perform regression with variable selection to get accurate
#       performance predictions

# TODO: implement web url not just local url

def parse_title_hints(task):
    kwargs = dict()
    if task.title:
        kwargs.update([ (k, v) for k, v in
                        re.findall(r'([a-z]+)\s*=\s*(\d+)', task.title())
                        if k in field_set ])
    return default_prediction._replace(**kwargs)

def hash_n_size(files_list):
    return [
        (str(hash(f)), os.stat(f).st_size/1024./1024)
        for f in files_list
    ]


class DummyPerformancePredictor(object):
    """Just make something that works, then do regression later"""

    def __init__(self, url):
        self.url = url
        self.state = dict()
        if os.path.exists(url):
            with open(url) as f_in:
                self.state = json.load(f_in)

    def update(self, task, max_rss_mb, cpu_hrs, clock_hrs):
        pass

    def predict(self, task):
        return parse_title_hints(task)

    def save(self):
        with open(self.url, 'w') as f:
            json.dump(self.state, f)


class LocalPerformancePredictor(DummyPerformancePredictor):
    pass


class WebPerformancePredictor(DummyPerformancePredictor):
    def __init__(self, url):
        self.url = url
        self.messages = list()
        
    def update(self, task, max_rss_mb, cpu_hrs, clock_hrs):
        self._send({
            "name": task.name, "max_rss_mb": max_rss_mb,
            "cpu_hrs": cpu_hrs, "clock_hrs": clock_hrs,
            "targets": hash_n_size(task.targets),
            "file_dep": hash_n_size(task.file_dep)
        })

    def _send(self, perf_dict):
        requests.post(self.url, data=json.dumps(perf_dict))

    def save(self):
        pass


def new_predictor(url=DEFAULT_URL):
    if url.startswith('http://'):
        return WebPerformancePredictor(url)
    elif url == "dummy":
        return DummyPerformancePredictor(url)
    else:
        return LocalPerformancePredictor(url)

