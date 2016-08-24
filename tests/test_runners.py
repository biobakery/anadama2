import os
import shutil
import unittest

import anadama2
import anadama2.tracked
import anadama2.runners
import anadama2.helpers


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
        t = anadama2.Task(
            "my task",
            actions=[anadama2.helpers.sh("wc -l /etc/hosts > "+outf)],
            depends=[anadama2.tracked.auto("/etc/hosts")],
            targets=[anadama2.tracked.auto(outf)],
            task_no=1
        )
        ret = anadama2.runners._run_task_locally(t)
        self.assertTrue(os.stat(outf).st_size > 0,
                        "should create an output file")
        self.assertTrue(isinstance(ret, anadama2.runners.TaskResult),
                        "_run_task_locally should return a TaskResult")
        self.assertIs(ret.error, None, "shouldn't return an error")

        t2 = t._replace(targets=[anadama2.tracked.auto(outf+".doesntexist")])
        ret = anadama2.runners._run_task_locally(t2)
        self.assertTrue(isinstance(ret, anadama2.runners.TaskResult),
                        ("_run_task_locally should return an TaskResult "
                         "even in case of failure"))
        self.assertIsNot(ret.error, None, "should have had an error")
        self.assertIn("Failed to produce target", ret.error,
                      "The error should say that I failed to produce a target")
        
        t3 = t._replace(actions=[anadama2.helpers.sh("exit 1")])
        ret = anadama2.runners._run_task_locally(t3)
        self.assertTrue(isinstance(ret, anadama2.runners.TaskResult),
                        ("_run_task_locally should return an TaskResult "
                         "even in case of failure"))
        self.assertIsNot(ret.error, None, "should have had an error")


