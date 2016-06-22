import os
import sys
import time
import shutil
import random
import unittest
from datetime import datetime
from datetime import timedelta
from cStringIO import StringIO

import networkx

import anadama
import anadama.deps
import anadama.runcontext
import anadama.util
import anadama.backends
import anadama.taskcontainer

from util import capture


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
        if self.ctx._backend:
            self.ctx._backend.close()
            del self.ctx._backend
            self.ctx._backend = None
            anadama.backends._default_backend = None
            
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        

    def test_hasattributes(self):
        self.assertIsInstance(self.ctx.dag,
                              networkx.classes.digraph.DiGraph)
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
                                   anadama.deps.KVDependency))
        
    def test_do_track_binaries(self):
        t1 = self.ctx.do("echo true", track_cmd=False)
        self.assertEqual(len(t1.depends), 2)
        self.assertEqual(len(t1.targets), 0)
        self.assertEqual(len(t1.actions), 1)
        self.assertTrue(isinstance(t1.depends[0],
                                   anadama.deps.ExecutableDependency))
        self.assertTrue(isinstance(t1.depends[1],
                                   anadama.deps.ExecutableDependency))


    def test_discover_binaries(self):
        bash_script = os.path.join(self.workdir, "test.sh")
        with open(bash_script, 'w') as f:
            print >> f, "#!/bin/bash"
            print >> f, "echo hi"
        os.chmod(bash_script, 0o755)
        plain_file = os.path.join(self.workdir, "blah.txt")
        with open(plain_file, 'w') as f:
            print >> f, "nothing to see here"
        ret = anadama.runcontext.discover_binaries("echo hi")
        self.assertGreater(len(ret), 0, "should find /bin/echo")
        self.assertTrue(isinstance(ret[0], anadama.deps.ExecutableDependency))
        self.assertEqual(str(ret[0]), "/bin/echo")

        ret2 = anadama.runcontext.discover_binaries("/bin/echo foo")
        self.assertIs(ret[0], ret2[0], "should discover the same dep")
        
        ret = anadama.runcontext.discover_binaries(
            bash_script+" arguments dont matter")
        self.assertEqual(len(ret), 1, "should just find one dep")
        self.assertTrue(isinstance(ret[0], anadama.deps.ExecutableDependency))
        self.assertEqual(str(ret[0]), bash_script)

        ret = anadama.runcontext.discover_binaries(plain_file+" blah blah")
        self.assertEqual(len(ret), 0, "shouldn't discover unexecutable files")
        

    def test_do_targets(self):
        t = self.ctx.do("echo true > @{true.txt}")
        self.assertIn("true.txt", t.name,
                      "target shouldn't be removed from command string")
        self.assertNotIn(t.name, "@{true.txt}", "target metachar not removed")
        self.assertEqual(len(t.targets), 1, "Should only be one target")
        self.assertEqual(os.path.basename(str(t.targets[0])), "true.txt",
                         "the target should be a filedependency, true.txt")

    def test_do_deps(self):
        def closure(*args, **kwargs):
            return args, kwargs
        self.ctx.add_task = closure
        args, kws = self.ctx.do("cat #{/etc/hosts} > @{hosts.txt}",
                                track_cmd=False, track_binaries=False)
        
        self.assertNotIn("@{hosts}", kws["targets"],
                         "targ metachar not removed")
        self.assertIn("hosts.txt", map(os.path.basename, kws["targets"]),
                      "targ should be in targets")
        self.assertIn("hosts.txt", kws['name'],
                      "targ shouldn't be removed from command string")
        self.assertIn("/etc/hosts", kws['name'],
                      "dep shouldn't be removed from command string")
        self.assertNotIn("#{/etc/hosts}", kws["depends"],
                         "dep metachar not removed")
        self.assertIn("/etc/hosts", kws["depends"], "dep should be in deps")
        self.assertEqual(len(kws["depends"]), 1, "Should only be one dep")
        self.assertEqual(kws["depends"][0], "/etc/hosts",
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
        
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertTrue(os.path.exists(outf), "should create wordcount.txt")
        
        
    def test_go_parallel(self):
        for _ in range(10):
            self.ctx.add_task("sleep 0.5")
        earlier = datetime.now()
        with capture(stderr=StringIO()):
            self.ctx.go(n_parallel=10)
        later = datetime.now()
        self.assertLess(later-earlier, timedelta(seconds=5))

    def test_go_quit_early(self):
        outf = os.path.join(self.workdir, "blah.txt")
        out2 = os.path.join(self.workdir, "shouldntexist.txt")
        self.ctx.add_task("echo blah > {targets[0]}; exit 1", targets=[outf])
        self.ctx.add_task("cat {depends[0]} > {targets[0]}",
                          depends=[outf], targets=[outf])

        with capture(stderr=StringIO()):
            with self.assertRaises(anadama.runcontext.RunFailed):
                self.ctx.go(quit_early=True)

        self.assertFalse(
            os.path.exists(out2),
            "quit_early failed to stop before the second task was run")

    def test_go_skip(self):
        outf = os.path.join(self.workdir, "blah.txt")
        self.ctx.add_task("touch {targets[0]}", targets=[outf])
        with capture(stderr=StringIO()):
            self.ctx.go()
        ctime = os.stat(outf).st_ctime
        time.sleep(1)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(ctime, os.stat(outf).st_ctime)


    def test_go_skip_notargets(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        self.ctx.add_task("touch {targets[0]}", targets=[a])
        self.ctx.add_task("touch {targets[0]} {targets[1]}", targets=[b, c])
        self.ctx.add_task("touch {}".format(d), depends=[a,b])
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(a).st_mtime
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(a).st_mtime)
        os.remove(a)
        time.sleep(0.01)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertNotEqual(mtime, os.stat(a).st_mtime)


    def test_go_skip_glob(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        globdep = anadama.deps.GlobDependency(
            os.path.join(self.workdir, "*.txt")
        )
        out = os.path.join(self.workdir, "out")
        self.ctx.already_exists(globdep)
        for letter in (a,b,c,d):
            anadama.util.sh("echo {a} > {a}".format(a=letter), shell=True)
        self.ctx.add_task("ls {depends[0]} > {targets[0]}",
                          depends=globdep, targets=out)
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(out).st_mtime
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(out).st_mtime)
        with open(os.path.join(self.workdir, "f.txt"), 'w') as f:
            print >> f, "hi mom"
        time.sleep(0.01)
        with capture(stderr=StringIO()):
            self.ctx.go()
        new_mtime = os.stat(out).st_mtime
        self.assertNotEqual(mtime, new_mtime)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(new_mtime, os.stat(out).st_mtime)


    def test_go_skip_dir(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        dirdep = anadama.deps.DirectoryDependency(self.workdir)
        out = "/tmp/foobaz"
        self.ctx.already_exists(dirdep)
        for letter in (a,b,c,d):
            anadama.util.sh("echo {a} > {a}".format(a=letter), shell=True)
        self.ctx.add_task("ls {depends[0]} > {targets[0]}",
                          depends=dirdep, targets=out)
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(out).st_mtime
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(out).st_mtime)
        with open(os.path.join(self.workdir, "f.txt"), 'w') as f:
            print >> f, "hi mom"
        time.sleep(0.01)
        with capture(stderr=StringIO()):
            self.ctx.go()
        new_mtime = os.stat(out).st_mtime
        self.assertNotEqual(mtime, new_mtime)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(new_mtime, os.stat(out).st_mtime)
        os.remove(out)


    def test_go_skip_config(self):
        a = os.path.join(self.workdir, "a.txt")
        conf = anadama.deps.KVContainer(alpha="5", beta=2)
        self.ctx.add_task("echo beta:{depends[0]} > {targets[0]}",
                          depends=conf.beta, targets=a)
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(a).st_mtime
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(a).st_mtime)
        conf.beta = 7
        time.sleep(0.01)
        with capture(stderr=StringIO()):
            self.ctx.go()
        new_mtime = os.stat(a).st_mtime
        self.assertNotEqual(mtime, new_mtime)
        conf.gamma = 10
        time.sleep(0.01)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(new_mtime, os.stat(a).st_mtime)
        self.ctx.add_task("echo beta > {targets[0]}",
                          depends=conf.items(), targets=a)
        time.sleep(0.01)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertNotEqual(new_mtime, os.stat(a).st_mtime)


    def test_issue36(self):
        ctx = self.ctx
        step1_const = anadama.deps.KVContainer(a = 12)
        step1_out = os.path.join(self.workdir, "step1.txt")
        step1_cmd = " ".join(["echo", str(step1_const.a), ">", step1_out])
        ctx.already_exists(anadama.deps.StringDependency(step1_cmd))
        step1 = ctx.add_task(step1_cmd,
                             depends=[step1_const.a,
                                      anadama.deps.StringDependency(step1_cmd)],
                             targets=[step1_out])

        step2_out = os.path.join(self.workdir, "step2.txt")
        step2_cmd = "; ".join(["p=$(cat " + step1_out + ")",
                               "echo $p > " + step2_out ])
        ctx.already_exists(anadama.deps.StringDependency(step2_cmd))

        step2 = ctx.add_task(step2_cmd,
                             depends=[step1_out],
                             targets=[step2_out],
                             name="Step 2")
        with capture(stderr=StringIO()):
            ctx.go()

        step2skipped = False
        class CustomReporter(anadama.reporters.ConsoleReporter):
            def task_skipped(self, task_no, *args, **kwargs):
                if task_no == step2.task_no:
                    step2skipped = True
                return super(CustomReporter, self).task_skipped(
                    task_no, *args, **kwargs)

        step1_const.a = 10
        with capture(stderr=StringIO()):
            ctx.go(reporter=CustomReporter(ctx))

        self.assertFalse(step2skipped,
                         "Shouldn't skip step 2; parent dep changed")


    def test_print_within_function_action(self):
        stderr_msg = "".join([chr(random.randint(32, 126)) for _ in range(10)])
        stdout_msg = "".join([chr(random.randint(32, 126)) for _ in range(10)])
        def printer(task):
            print >> sys.stderr, stderr_msg
            print >> sys.stdout, stdout_msg

        t1 = self.ctx.add_task(printer)

        out, err = StringIO(), StringIO()
        with capture(stdout=out, stderr=err):
            self.ctx.go()

        self.assertNotIn(t1.task_no, self.ctx.failed_tasks)
        self.assertIn(stdout_msg, out.getvalue())
        self.assertIn(stderr_msg, err.getvalue())
       
    def test__sugar_list(self):
        rc = anadama.runcontext
        self.assertEqual([5], rc.sugar_list(5))
        self.assertEqual(["blah"], rc.sugar_list("blah"))
        it = iter(range(5))
        self.assertIs(it, rc.sugar_list(it))
        t = anadama.Task("dummy task", [], [], [], 0)
        self.assertEqual([t], rc.sugar_list(t))
        self.assertEqual((5,), rc.sugar_list((5,)))


    def test_add_task_custom_dependency(self):
        class CustomDependency(anadama.deps.BaseDependency):
            @staticmethod
            def key(the_key):
                return str(the_key)

            def compare(self):
                self.compared = True
                yield str(self._key)

            def init(self, key):
                self.initialized = True
                self.compared = False

        d = CustomDependency("blah")
        self.ctx.add_task(anadama.util.noop, targets=d)
        self.assertTrue(d.initialized)
        self.assertFalse(d.compared)
        with capture(stdout=StringIO(), stderr=StringIO()):
            self.ctx.go()
        self.assertTrue(d.compared)


    def test_TaskContainer(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        t1 = self.ctx.add_task("touch {targets[0]}", targets=[a], name="a")
        t2 = self.ctx.add_task("touch {targets[0]} {targets[1]}",
                               targets=[b, c], name="bc")
        t3 = self.ctx.add_task("touch {}".format(d), depends=[a,b], name="d")
        self.assertTrue(isinstance(self.ctx.tasks, anadama.taskcontainer.TaskContainer))
        self.assertIs(self.ctx.tasks[t1.task_no], t1)
        self.assertIs(self.ctx.tasks[t2.name], t2)


if __name__ == "__main__":
    unittest.main()
