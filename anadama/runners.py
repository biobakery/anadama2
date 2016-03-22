import time
import traceback
import multiprocessing
import cPickle as pickle
from collections import namedtuple

from .pickler import cloudpickle

class TaskResult(namedtuple(
        "TaskResult", ["task_no", "error", "dep_keys", "dep_compares"])):
    """ TODO: doc
    """
    pass



class TaskFailed(Exception):
    def __init__(self, msg, task_no):
        self.task_no = task_no
        super(TaskFailed, self).__init__(msg)

class BaseRunner(object):
    def __init__(self, run_context):
        self.ctx = run_context
        self.quit_early = False

    def run_tasks(self, task_idx_deque):
        raise NotImplementedError()


class SerialLocalRunner(BaseRunner):

    def run_tasks(self, task_idx_deque):
        total = len(self.ctx.tasks)
        while total > len(self.ctx.failed_tasks)+len(self.ctx.completed_tasks):
            idx = task_idx_deque.pop()

            parents = set(self.ctx.dag.predecessors(idx))
            failed_parents = parents.intersection(self.ctx.failed_tasks)
            if failed_parents:
                self.ctx._handle_task_result(
                    parent_failed_result(idx, iter(failed_parents).next()))
                continue

            self.ctx._handle_task_started(idx)
            result = _run_task_locally(self.ctx.tasks[idx])
            self.ctx._handle_task_result(result)

            if self.quit_early and bool(result.error):
                break


logger = multiprocessing.log_to_stderr()
logger.setLevel(multiprocessing.SUBWARNING)

class ParallelLocalWorker(multiprocessing.Process):
    def __init__(self, work_q, result_q):
        super(ParallelLocalWorker, self).__init__()
        self.logger = logger
        self.work_q = work_q
        self.result_q = result_q


    def run(self):
        self.logger.debug("Starting worker")
        while True:
            try:
                self.logger.debug("Getting work")
                pkl = self.work_q.get()
                self.logger.debug("Got work")
            except IOError as e:
                self.logger.debug("Received IOError (%s) errno %s from work_q",
                                  e.message, e.errno)
                break
            except EOFError:
                self.logger.debug("Received EOFError from work_q")
                break
            if type(pkl) is dict and pkl.get("stop", False):
                self.logger.debug("Received sentinel, stopping")
                break
            try:
                self.logger.debug("Deserializing task")
                task = pickle.loads(pkl)
                self.logger.debug("Task deserialized")
            except Exception as e:
                self.result_q.put_nowait(exception_result(e))
                self.logger.debug("Failed to deserialize task")
                continue
            self.logger.debug("Running task locally")
            result = _run_task_locally(task)
            self.logger.debug("Finished running task; "
                              "putting results on result_q")
            self.result_q.put_nowait(result)
            self.logger.debug("Result put on result_q. Back to get more work.")
            


class ParallelLocalRunner(BaseRunner):
    MAX_QSIZE = 1000

    def __init__(self, run_context, n_parallel):
        super(ParallelLocalRunner, self).__init__(run_context)
        self.work_q = multiprocessing.Queue(self.MAX_QSIZE)
        self.result_q = multiprocessing.Queue(self.MAX_QSIZE)
        self.workers = [ ParallelLocalWorker(self.work_q, self.result_q)
                         for _ in range(n_parallel) ]
        self.started = False


    def run_tasks(self, task_idx_deque):
        self.task_idx_deque = task_idx_deque
        total = len(self.ctx.tasks)

        while True:
            n_filled = self._fill_work_q()
            n_done = len(self.ctx.failed_tasks)+len(self.ctx.completed_tasks)
            if n_filled == 0 and n_done >= total:
                break
            try:
                result = self.result_q.get()
            except (SystemExit, KeyboardInterrupt, Exception):
                self.terminate()
                raise
            else:
                self.ctx._handle_task_result(result)
            n_filled -= 1
            if self.quit_early and result.error:
                self.terminate()
                break

        self.cleanup()


    def _fill_work_q(self):
        logger.debug("Filling work_q")
        n_filled = 0
        for _ in range(min(self.MAX_QSIZE, len(self.task_idx_deque))):
            idx = self.task_idx_deque.pop()
            parents = set(self.ctx.dag.predecessors(idx))
            failed_parents = parents.intersection(self.ctx.failed_tasks)
            if failed_parents:
                self.ctx._handle_task_result(
                    parent_failed_result(idx, failed_parents[0]))
                continue
            elif parents.intersection(self.ctx.completed_tasks):
                # has undone parents, come back again later
                self.task_idx_deque.appendleft(idx)
                continue
            try:
                pkl = cloudpickle.dumps(self.ctx.tasks[idx])
            except Exception as e:
                msg = ("Unable to serialize task `{}'. "
                       "Original error was `{}'.")
                raise ValueError(msg.format(self.ctx.tasks[idx], e))
            logger.debug("Adding task %i to work_q", idx)
            self.ctx._handle_task_started(idx)
            self.work_q.put(pkl)
            logger.debug("Added task %i to work_q", idx)
            n_filled += 1
        if not self.started:
            logger.debug("Starting up workers")
            for w in self.workers:
                w.start()
            self.started = True
        return n_filled


    def terminate(self):
        self.work_q._rlock.acquire()
        while self.work_q._reader.poll():
            try:
                self.work_q._reader.recv()
            except EOFError:
                break
            time.sleep(0)
        for worker in self.workers:
            worker.terminate()
        self.cleanup()


    def cleanup(self):
        for w in self.workers:
            self.work_q.put({"stop": True})
        for w in self.workers:
            w.join()



def default(run_context, n_parallel):
    if n_parallel < 2:
        return SerialLocalRunner(run_context)
    else:
        return ParallelLocalRunner(run_context, n_parallel)


def exception_result(exc):
    return TaskResult(getattr(exc, "task_no", None), exc.message, None, None)

def parent_failed_result(idx, parent_idx):
    return TaskResult(
        idx, "Task failed because parent task `{}' failed".format(parent_idx),
        None, None)


def _run_task_locally(task):
    for i, action_func in enumerate(task.actions):
        try:
            action_func(task)
        except Exception:
            msg = ("Error executing action {}. "
                   "Original Exception: \n{}")
            return exception_result(
                TaskFailed(msg.format(i, traceback.format_exc()), task.task_no)
                )

    targ_keys, targ_compares = list(), list()
    for target in task.targets:
        targ_keys.append(target._key)
        try:
            targ_compares.append(list(target.compare()))
        except Exception:
            msg = "Failed to produce target `{}'. Original exception: {}"
            return exception_result(
                TaskFailed(msg.format(target, traceback.format_exc()),
                           task.task_no)
                )
            
    return TaskResult(task.task_no, None, targ_keys, targ_compares)


        

    
