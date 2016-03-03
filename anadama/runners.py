import traceback
import cPickle as pickle
from collections import namedtuple


class TaskResult(namedtuple(
        "TaskResult", ["task_no", "error", "dep_keys", "dep_compares"])):
    """ TODO: doc
    """
    pass



class TaskFailed(Exception):
    def __init__(self, msg, task_no):
        self.task_no = task_no
        super(TaskFailed, self).__init__(msg)



def default():
    return run_local


def exception_result(exc):
    return TaskResult(exc.task_no, exc.msg, None, None)


def _run_task_locally(task_pkl):
    task = pickle.loads(task_pkl)
    for i, action_func in enumerate(task.actions):
        try:
            action_func()
        except Exception as e:
            msg = ("Error executing action {}. "
                   "Original Exception: \n{}\n").format(i, e)
            msg += "\n".join(traceback.format_stack())
            raise TaskFailed(msg, task.task_no)

    targ_keys, targ_compares = list(), list()
    for target in task.targets:
        targ_keys.append(target._key)
        try:
            targ_compares.append(list(target.compare()))
        except Exception as e:
            msg = "Failed to produce target `{}'. Original exception: {}"
            raise TaskFailed(msg.format(target, e), task.task_no)
            
    return TaskResult(task.task_no, None, targ_keys, targ_compares)


def run_local(work_q, res_q):
    while True:
        pkl = work_q.get()
        try:
            result = _run_task_locally(pkl)
        except Exception as e:
            result = exception_result(e, pkl)
        res_q.put(result)
        work_q.task_done()
        

def run_lsf(workf_q, res_q):
    pass

def run_slurm(workf_q, res_q):
    pass

def run_sge(workf_q, res_q):
    pass
    
