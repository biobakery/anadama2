import cPickle as pickle
from collections import namedtuple

class TaskResult(namedtuple(
        "TaskResult", ["task_no", "errors", "dep_keys", "dep_compares"])):
    """ TODO: doc
    """
    pass


def default():
    pass

def exception_result(exc, pkl):
    return TaskResult(pickle.loads(pkl).task_no, str(exc), None, None)

def run_task_locally(task_pkl):
    task = pickle.loads(task_pkl)


def LocalRunner(work_q, res_q):
    while True:
        pkl = work_q.get()
        try:
            result = run_task_locally(pkl)
        except Exception as e:
            result = exception_result(e, pkl)
        res_q.put(result)
        work_q.task_done()
        

def LSFRunner(workf_q, res_q):
    pass

def SGERunner(GridRunner):
    pass

def SLURMRunner(GridRunner):
    pass
    
