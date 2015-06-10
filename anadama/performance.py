import os
import json
from collections import namedtuple

DEFAULT_URL = "./.anadama_performance_history.json"
DEFAULT_MEM = 100*1024 # 10GB in KB
# DEFAULT_MEM = 10 * 1024 * 1024 # 10GB in KB
DEFAULT_TIME = 2 #DELETEME
# DEFAULT_TIME = 20 * 60 # 20 hrs in mins

Prediction = namedtuple("Prediction", "mem time")

# TODO: actually implement history

# TODO: perform regression with variable selection to get accurate
#       performance predictions

# TODO: implement web url not just local url


class PerformancePredictor(object):
    """Just make something that works, then do regression later"""

    def __init__(self, url=DEFAULT_URL):
        if not url:
            url = DEFAULT_URL
        self.state = dict()
        if os.path.exists(url):
            with open(url) as f_in:
                self.state = json.load(f_in)

    def update(self, task, max_rss_kb, cpu_time):
        pass

    def predict(self, task):
        return Prediction(time=DEFAULT_TIME, mem=DEFAULT_MEM)

    def save(self):
        with open(self.url, 'w') as f:
            json.dump(self.state, f)
    
