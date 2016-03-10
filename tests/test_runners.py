import os
import shutil
import unittest

import anadama
import anadama.deps
import anadama.runners
import anadama.helpers
from anadama.pickler import cloudpickle


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
        ret = anadama.runners._run_task_locally(t)
        self.assertTrue(os.stat(outf).st_size > 0,
                        "should create an output file")
        self.assertTrue(isinstance(ret, anadama.runners.TaskResult),
                        "_run_task_locally should return a TaskResult")
        self.assertIs(ret.error, None, "shouldn't return an error")

        t2 = t._replace(targets=[anadama.deps.auto(outf+".doesntexist")])
        ret = anadama.runners._run_task_locally(t2)
        self.assertTrue(isinstance(ret, anadama.runners.TaskResult),
                        ("_run_task_locally should return an TaskResult "
                         "even in case of failure"))
        self.assertIsNot(ret.error, None, "should have had an error")
        self.assertIn("Failed to produce target", ret.error,
                      "The error should say that I failed to produce a target")
        
        t3 = t._replace(actions=[anadama.helpers.sh("exit 1")])
        ret = anadama.runners._run_task_locally(t3)
        self.assertTrue(isinstance(ret, anadama.runners.TaskResult),
                        ("_run_task_locally should return an TaskResult "
                         "even in case of failure"))
        self.assertIsNot(ret.error, None, "should have had an error")


