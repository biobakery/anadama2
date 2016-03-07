import os
import shutil
import unittest
import multiprocessing

import anadama
import anadama.deps
import anadama.runners
import anadama.helpers
from anadama.pickler import cloudpickle

def timed(func, time, *args, **kwargs):
    p = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
    p.start()
    p.join(time)
    if p.is_alive():
        p.terminate()
        raise Exception("Timed out!")


class TestRunners(unittest.TestCase):
    
    def setUp(self):
        self.workdir = "/tmp/anadama_testdir"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)

    def tearDown(self):
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        

    def test__run_task_locally(self):
        outf = os.path.join(self.workdir, "wc.txt")
        t = anadama.Task(
            "my task",
            actions=[anadama.helpers.sh("wc -l /etc/hosts > "+outf)],
            depends=[anadama.deps.auto("/etc/hosts")],
            targets=[anadama.deps.auto(outf)],
            task_no=1
        )
        ret = anadama.runners._run_task_locally(cloudpickle.dumps(t))
        self.assertTrue(os.stat(outf).st_size > 0,
                        "should create an output file")
        self.assertTrue(isinstance(ret, anadama.runners.TaskResult),
                        "_run_task_locally should return a TaskResult")
        self.assertIs(ret.error, None, "shouldn't return an error")

        t2 = t._replace(targets=[anadama.deps.auto(outf+".doesntexist")])
        pkl = cloudpickle.dumps(t2)
        with self.assertRaises(anadama.runners.TaskFailed):
            anadama.runners._run_task_locally(pkl)

        t3 = t._replace(actions=[anadama.helpers.sh("exit 1")])
        pkl = cloudpickle.dumps(t3)
        with self.assertRaises(anadama.runners.TaskFailed):
            anadama.runners._run_task_locally(pkl)


    def test_run_local(self):
        outf = os.path.join(self.workdir, "wc.txt")
        t = anadama.Task(
            "my task",
            actions=[anadama.helpers.sh("wc -l /etc/hosts > "+outf)],
            depends=[anadama.deps.auto("/etc/hosts")],
            targets=[anadama.deps.auto(outf)],
            task_no=1
        )
        wq = multiprocessing.Queue()
        rq = multiprocessing.Queue()
        wq.put(cloudpickle.dumps(t))
        anadama.runners.run_local(wq, rq)
        ret = rq.get(False, 3)
        self.assertTrue(isinstance(ret, anadama.runners.TaskResult))
        self.assertIs(ret.errors, None)
        
