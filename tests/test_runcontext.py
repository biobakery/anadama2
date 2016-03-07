import os
import shutil

import unittest

import networkx

import anadama
import anadama.deps
import anadama.runcontext
import anadama.util
import anadama.backends

class TestRunContext(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ[anadama.backends.ENV_VAR] = "/tmp/anadamatest"
    
    @classmethod
    def tearDownClass(cls):
        if os.path.isdir("/tmp/anadamatest"):
            shutil.rmtree("/tmp/anadamatest")


    def setUp(self):
        self.ctx = anadama.runcontext.RunContext()
        self.workdir = "/tmp/anadama_testdir"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)

    def tearDown(self):
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        

    def test_hasattributes(self):
        self.assertIsInstance(self.ctx.dag,
                              networkx.classes.digraph.DiGraph)
        self.assertIs(type(self.ctx.tasks), list)
        self.assertTrue(hasattr(self.ctx, "task_counter"))

    def test_do_simple(self):
        t1 = self.ctx.do("echo true", track_cmd=False, track_binaries=False)
        self.assertTrue(isinstance(t1, anadama.Task))
        self.assertIs(t1, self.ctx.tasks[0])
        self.assertEqual(len(t1.depends), 0)
        self.assertEqual(len(t1.targets), 0)
        self.assertEqual(len(t1.actions), 1)

    def test_do_track_cmd(self):
        t1 = self.ctx.do("echo true", track_binaries=False)
        self.assertEqual(len(t1.depends), 1)
        self.assertEqual(len(t1.targets), 0)
        self.assertEqual(len(t1.actions), 1)
        self.assertTrue(isinstance(t1.depends[0],
                                   anadama.deps.StringDependency))
        
    def test_do_track_binaries(self):
        t1 = self.ctx.do("echo true", track_cmd=False)
        self.assertEqual(len(t1.depends), 2)
        self.assertEqual(len(t1.targets), 0)
        self.assertEqual(len(t1.actions), 1)
        self.assertTrue(isinstance(t1.depends[0],
                                   anadama.deps.ExecutableDependency))
        self.assertTrue(isinstance(t1.depends[1],
                                   anadama.deps.ExecutableDependency))

    def test_do_targets(self):
        def closure(*args, **kwargs):
            return args, kwargs
        self.ctx.add_task = closure
        args, kws = self.ctx.do("echo true > @{true.txt}")
        self.assertIn("true.txt", args[0],
                      "target shouldn't be removed from command string")
        self.assertNotIn(args[0], "@{true.txt}", "target metachar not removed")
        self.assertEqual(len(args[2]), 1, "Should only be one target")
        self.assertEqual(args[2][0], "true.txt",
                         "the target should be a filedependency, true.txt")

    def test_do_deps(self):
        def closure(*args, **kwargs):
            return args, kwargs
        self.ctx.add_task = closure
        args, kws = self.ctx.do("cat #{/etc/hosts} > @{hosts.txt}",
                                track_cmd=False, track_binaries=False)
        self.assertNotIn("#{/etc/hosts}", args[1],
                         "dep metachar not removed")
        self.assertIn("/etc/hosts", args[0],
                      "dep shouldn't be removed from command string")
        self.assertEqual(len(args[1]), 1, "Should only be one dep")
        self.assertEqual(args[1][0], "/etc/hosts",
                         "the dep should be a filedependency, /etc/hosts")


    
    def test_add_task(self):
        t1 = self.ctx.add_task(anadama.util.noop)
        self.assertIsInstance(t1, anadama.Task)
        self.assertIs(t1.actions[0], anadama.util.noop)
        self.assertEqual(len(t1.depends), 0)
        self.assertEqual(len(t1.targets), 0)
        

    def test_add_task_deps(self):
        self.ctx.already_exists("/etc/hosts")
        t1 = self.ctx.add_task(anadama.util.noop, depends=["/etc/hosts"])
        self.assertEqual(len(t1.depends), 1)
        self.assertEqual(len(t1.targets), 0)
        self.assertIs(t1.depends[0], anadama.deps.FileDependency("/etc/hosts"),
                      "the dep should be a filedependency, /etc/hosts")


    def test_add_task_targs(self):
        t1 = self.ctx.add_task(anadama.util.noop, targets=["/tmp/test.txt"])
        self.assertEqual(len(t1.depends), 0)
        self.assertEqual(len(t1.targets), 1)
        self.assertIs(t1.targets[0],
                      anadama.deps.FileDependency("/tmp/test.txt"),
                      "the target should be a filedependency, /tmp/test.txt")

    def test_add_task_decorator(self):
        ctx = self.ctx
        @ctx.add_task(targets=["/tmp/test.txt"])
        def closure(*args, **kwargs):
            return "testvalue"

        self.assertEqual(len(ctx.tasks), 1, "decorator should add one task")
        self.assertIsInstance(
            ctx.tasks[0], anadama.Task,
            "decorator should add a task instance to context.tasks")
        self.assertEqual(len(ctx.tasks[0].targets), 1,
                         "The created task should have one target")
        self.assertIs(ctx.tasks[0].targets[0],
                      anadama.deps.FileDependency("/tmp/test.txt"),
                      "the target should be a filedependency, /tmp/test.txt")
        ret = ctx.tasks[0].actions[0]()
        self.assertEqual(ret, "testvalue",
                         "the action should be the same function I gave it")


    def test_go(self):
        self.ctx.already_exists("/etc/hosts")
        outf = os.path.join(self.workdir, "wordcount.txt")
        self.ctx.add_task("wc -l {depends[0]} > {targets[0]}",
                          depends=["/etc/hosts"], targets=[outf] )
        self.ctx.go()
        self.assertTrue(os.path.exists(outf), "should create wordcount.txt")
        
        

        

if __name__ == "__main__":
    unittest.main()
