# -*- coding: utf-8 -*-
import os
import sys
import time
import shutil
import random
import unittest
from datetime import datetime
from datetime import timedelta
from io import StringIO

import six
import networkx

import anadama2
import anadama2.tracked
import anadama2.workflow
import anadama2.util
import anadama2.cli
import anadama2.backends
import anadama2.taskcontainer
from anadama2.util import capture


SLEEPTIME = os.environ.get("ANADAMA_SLEEP_TIME", "0.01")
SLEEPTIME=float(SLEEPTIME)

class TestWorkflow(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ[anadama2.backends.ENV_VAR] = "/tmp/anadamatest"
    
    @classmethod
    def tearDownClass(cls):
        if os.path.isdir("/tmp/anadamatest"):
            shutil.rmtree("/tmp/anadamatest")


    def setUp(self):
        self.workdir = "/tmp/anadama_testdir"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)
        cfg = anadama2.cli.Configuration(prompt_user=False)
        cfg._arguments["output"].keywords["default"]=self.workdir
        self.ctx = anadama2.workflow.Workflow(vars=cfg)


    def tearDown(self):
        if self.ctx._backend:
            self.ctx._backend.close()
            del self.ctx._backend
            self.ctx._backend = None
            anadama2.backends._default_backend = None
            
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        

    def test_hasattributes(self):
        self.assertIsInstance(self.ctx.dag,
                              networkx.classes.digraph.DiGraph)
        self.assertTrue(hasattr(self.ctx, "task_counter"))
        
    def test_get_input_files(self):
        # check that the input files function reads 
        # the directory and returns a list
        self.assertTrue(isinstance(self.ctx.get_input_files(), list))
        
    def test_name_output_files_single_file(self):
        # check that the output files function 
        # returns a single file as a string
        output_file=self.ctx.name_output_files(name="one.txt",tag="new",extension="tsv")
        self.assertEqual("one_new.tsv", os.path.basename(output_file))
        
    def test_name_output_files_multiple_files(self):
        # check that the output files function 
        # returns a list of two files
        output_files=self.ctx.name_output_files(name=["one.txt","two.txt"],
            subfolder="new", extension="tsv")
        self.assertEqual("one.tsv", os.path.basename(output_files[0]))

    def test_do_simple(self):
        t1 = self.ctx.do("echo true", track_cmd=False, track_binaries=False)
        self.assertTrue(isinstance(t1, anadama2.Task))
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
                                   anadama2.tracked.TrackedVariable))
        
    def test_do_track_binaries(self):
        t1 = self.ctx.do("echo true", track_cmd=False)
        self.assertEqual(len(t1.depends), 2)
        self.assertEqual(len(t1.targets), 0)
        self.assertEqual(len(t1.actions), 1)
        self.assertTrue(isinstance(t1.depends[0],
                                   anadama2.tracked.TrackedExecutable))
        self.assertTrue(isinstance(t1.depends[1],
                                   anadama2.tracked.TrackedExecutable))


    def test_discover_binaries(self):
        bash_script = os.path.join(self.workdir, "test.sh")
        echoprog = anadama2.util.sh(("which", "echo"))[0].strip().decode("utf-8")
        with open(bash_script, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("echo hi\n")
        os.chmod(bash_script, 0o755)
        plain_file = os.path.join(self.workdir, "blah.txt")
        with open(plain_file, 'w') as f:
            f.write("nothing to see here\n")
        ret = anadama2.workflow.discover_binaries("echo hi")
        self.assertGreater(len(ret), 0, "should find "+echoprog)
        self.assertTrue(isinstance(ret[0], anadama2.tracked.TrackedExecutable))
        self.assertEqual(str(ret[0]), echoprog)

        ret2 = anadama2.workflow.discover_binaries(echoprog+" foo")
        self.assertIs(ret[0], ret2[0], "should discover the same dep")
        
        ret = anadama2.workflow.discover_binaries(
            bash_script+" arguments dont matter")
        self.assertEqual(len(ret), 1, "should just find one dep")
        self.assertTrue(isinstance(ret[0], anadama2.tracked.TrackedExecutable))
        self.assertEqual(str(ret[0]), bash_script)

        ret = anadama2.workflow.discover_binaries(plain_file+" blah blah")
        self.assertEqual(len(ret), 0, "shouldn't discover unexecutable files")

        ret = anadama2.workflow.discover_binaries("ls /bin/")
        self.assertEqual(len(ret), 1, "shouldn't discover directories")

        

    def test_do_targets(self):
        t = self.ctx.do("echo true > [t:true.txt]")
        self.assertIn("true.txt", t.name,
                      "target shouldn't be removed from command string")
        self.assertNotIn("[t:true.txt]",t.name, "target markup unchanged")
        self.assertNotIn(":", t.name, "target markup colon not removed")
        self.assertFalse("[" in t.name or "]" in t.name,
                         "target markup square brackets not removed")
        self.assertEqual(len(t.targets), 1, "Should only be one target")
        self.assertEqual(os.path.basename(str(t.targets[0])), "true.txt",
                         "the target should be a filedependency, true.txt")

    def test_do_deps(self):
        def closure(*args, **kwargs):
            return args, kwargs
        self.ctx.add_task = closure
        args, kws = self.ctx.do("cat [d:/etc/hosts] > [t:hosts.txt]",
                                track_cmd=False, track_binaries=False)

        self.assertNotIn("[t:hosts]", kws["targets"], 
                         "targ markup not removed")
        self.assertIn("hosts.txt", list(map(os.path.basename, kws["targets"])),
                      "targ should be in targets")
        self.assertIn("hosts.txt", kws['name'],
                      "targ shouldn't be removed from command string")
        self.assertIn("/etc/hosts", kws['name'],
                      "dep shouldn't be removed from command string")
        self.assertNotIn("[d:/etc/hosts]", kws["depends"],
                         "dep markup not removed")
        self.assertNotIn(":", kws['name'], "markup colon not removed")
        self.assertFalse("[" in kws['name'] or "]" in kws['name'],
                         "target markup square brackets not removed")
        self.assertIn("/etc/hosts", kws["depends"], "dep should be in deps")
        self.assertEqual(len(kws["depends"]), 1, "Should only be one dep")
        self.assertEqual(kws["depends"][0], "/etc/hosts",
                         "the dep should be a filedependency, /etc/hosts")


    def test_do_vars(self):
        def closure(*args, **kwargs):
            return args, kwargs
        self.ctx.add_task = closure
        args, kws = self.ctx.do("cat [d:/etc/hosts] > [v:output]/[t:hosts.txt]",
                                track_cmd=False, track_binaries=False)

        self.assertNotIn("[v:output]", kws["depends"],
                         "var markup not removed")
        self.assertNotIn(":", kws['name'], "markup colon not removed")
        self.assertFalse("[" in kws['name'] or "]" in kws['name'],
                         "target markup square brackets not removed")
        self.assertIn(self.ctx.vars.output, kws["name"],
                      "output variable should be replaced with the var value")
        self.assertNotIn("output", kws["name"],
                         "output variable should be replaced")

        
    def test_do_mixmatch(self):
        def closure(*args, **kwargs):
            return args, kwargs
        self.ctx.add_task = closure
        args, kws = self.ctx.do("ls -alh [vd:output]",
                                track_cmd=False, track_binaries=False)

        self.assertNotIn("[vd:output]", kws["depends"],
                         "var markup not removed")
        self.assertNotIn(":", kws['name'], "markup colon not removed")
        self.assertFalse("[" in kws['name'] or "]" in kws['name'],
                         "target markup square brackets not removed")
        self.assertNotIn("vd", kws['name'], "vd markup not removed")
        self.assertIn(self.ctx.vars.output, kws["name"],
                      "output variable should be replaced with the var value")
        self.assertNotIn("output", kws["name"],
                         "output variable should be replaced")
        self.assertIn(self.ctx.vars.output, kws['depends'],
                      "output variable not tracked as dependency")
        

    def test_do_mixmatch_transpose(self):
        def closure(*args, **kwargs):
            return args, kwargs
        self.ctx.add_task = closure
        args, kws = self.ctx.do("ls -alh [dv:output]",
                                track_cmd=False, track_binaries=False)

        self.assertNotIn("[dv:output]", kws["depends"],
                         "var markup not removed")
        self.assertNotIn(":", kws['name'], "markup colon not removed")
        self.assertFalse("[" in kws['name'] or "]" in kws['name'],
                         "target markup square brackets not removed")
        self.assertNotIn("dv", kws['name'], "vd markup not removed")
        self.assertIn(self.ctx.vars.output, kws["name"],
                      "output variable should be replaced with the var value")
        self.assertNotIn("output", kws["name"],
                         "output variable should be replaced")
        self.assertIn(self.ctx.vars.output, kws['depends'],
                      "output variable not tracked as dependency")
        
    
    def test_add_task(self):
        t1 = self.ctx.add_task(anadama2.util.noop)
        self.assertIsInstance(t1, anadama2.Task)
        self.assertIs(t1.actions[0], anadama2.util.noop)
        self.assertEqual(len(t1.depends), 0)
        self.assertEqual(len(t1.targets), 0)
        
    def test_add_task_group(self):
        self.ctx.add_task_group(anadama2.util.noop,
            depends=["/etc/hosts","/etc/hosts"],
            targets=["/tmp/test.txt","/tmp/test.txt"])
        # check for the two tasks plus the track pre-existing dependencies task
        self.assertEqual(len(self.ctx.tasks), 3)

    def test_add_task_deps(self):
        self.ctx.already_exists("/etc/hosts")
        t1 = self.ctx.add_task(anadama2.util.noop, depends=["/etc/hosts"])
        self.assertEqual(len(t1.depends), 1)
        self.assertEqual(len(t1.targets), 0)
        self.assertIs(t1.depends[0], anadama2.tracked.HugeTrackedFile("/etc/hosts"),
                      "the dep should be a filedependency, /etc/hosts")


    def test_add_task_targs(self):
        t1 = self.ctx.add_task(anadama2.util.noop, targets=["/tmp/test.txt"])
        self.assertEqual(len(t1.depends), 0)
        self.assertEqual(len(t1.targets), 1)
        self.assertIs(t1.targets[0],
                      anadama2.tracked.HugeTrackedFile("/tmp/test.txt"),
                      "the target should be a filedependency, /tmp/test.txt")


    def test_add_task_decorator(self):
        ctx = self.ctx
        @ctx.add_task(targets=["/tmp/test.txt"])
        def closure(*args, **kwargs):
            return "testvalue"

        self.assertEqual(len(ctx.tasks), 1, "decorator should add one task")
        self.assertIsInstance(
            ctx.tasks[0], anadama2.Task,
            "decorator should add a task instance to context.tasks")
        self.assertEqual(len(ctx.tasks[0].targets), 1,
                         "The created task should have one target")
        self.assertIs(ctx.tasks[0].targets[0],
                      anadama2.tracked.HugeTrackedFile("/tmp/test.txt"),
                      "the target should be a filedependency, /tmp/test.txt")
        ret = ctx.tasks[0].actions[0]()
        self.assertEqual(ret, "testvalue",
                         "the action should be the same function I gave it")


    def test_add_task_kwargs(self):
        outf = os.path.join(self.workdir, "test.txt")
        t1 = self.ctx.add_task("echo [msg] > [targets[0]]",
                               targets=outf, msg="foobar")
        self.assertEqual(len(t1.depends), 0)
        self.assertEqual(len(t1.targets), 1)
        self.assertIs(t1.targets[0],
                      anadama2.tracked.HugeTrackedFile(outf),
                      "the target should be a filedependency test.txt")
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertTrue(os.path.exists(outf), "should create test.txt")
        with open(outf) as f:
            data = f.read().strip()
        if six.PY3:
            self.assertEqual(data, "foobar")
        else:
            self.assertEquals(data, "foobar")


    def test_go(self):
        self.ctx.already_exists("/etc/hosts")
        outf = os.path.join(self.workdir, "wordcount.txt")
        self.ctx.add_task("wc -l [depends[0]] > [targets[0]]",
                          depends=["/etc/hosts"], targets=[outf] )
        
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertTrue(os.path.exists(outf), "should create wordcount.txt")
        
        
    def test_go_parallel(self):
        for _ in range(10):
            self.ctx.add_task("sleep 0.5")
        earlier = datetime.now()
        with capture(stderr=StringIO()):
            self.ctx.go(jobs=10)
        later = datetime.now()
        self.assertLess(later-earlier, timedelta(seconds=5))


    def test_issue1(self):
        a,b,c,d,e,f = [os.path.join(self.workdir, letter+".txt")
                       for letter in ("a", "b", "c", "d", "e", "f")]
        self.ctx.add_task("touch [targets[0]]", targets=[a], name="a")
        self.ctx.add_task("touch [targets[0]]; exit 1", depends=[a], targets=[b], name="task should fail")
        self.ctx.add_task("touch [targets[0]]", depends=[b], targets=[c], name="c")
        self.ctx.add_task("touch [targets[0]]", targets=[d], name="d")
        self.ctx.add_task("touch [targets[0]]", depends=[d], targets=[e], name="e")
        self.ctx.add_task("touch [targets[0]]", depends=[e], targets=[f], name="f")
        with capture(stderr=StringIO()):
            with self.assertRaises(anadama2.workflow.RunFailed):
                self.ctx.go(jobs=2)
        

    def test_go_quit_early(self):
        outf = os.path.join(self.workdir, "blah.txt")
        out2 = os.path.join(self.workdir, "shouldntexist.txt")
        self.ctx.add_task("echo blah > [targets[0]]; exit 1", targets=[outf], name="task should fail")
        self.ctx.add_task("cat [depends[0]] > [targets[0]]",
                          depends=[outf], targets=[outf])

        with capture(stderr=StringIO()):
            with self.assertRaises(anadama2.workflow.RunFailed):
                self.ctx.go(quit_early=True)

        self.assertFalse(
            os.path.exists(out2),
            "quit_early failed to stop before the second task was run")


    def test_go_until_task(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        self.ctx.add_task("touch [targets[0]]", targets=[a], name="a")
        self.ctx.add_task("touch [targets[0]] [targets[1]]", targets=[b, c], name="bc")
        self.ctx.add_task("touch "+d, depends=[a,b], name="d")
        with capture(stderr=StringIO()):
            self.ctx.go(until_task="bc")
        self.assertTrue(os.path.exists(a), "should quit at bc")
        self.assertTrue(os.path.exists(b), "should quit at bc")
        self.assertTrue(os.path.exists(c), "should quit at bc")
        self.assertFalse(os.path.exists(d), "should quit at bc")
        

    def test_go_exclude_task(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        self.ctx.add_task("touch [targets[0]]", targets=[a], name="a")
        self.ctx.add_task("touch [targets[0]] [targets[1]]", targets=[b, c], name="bc")
        self.ctx.add_task("touch "+d, depends=[a,b], name="d")
        with capture(stderr=StringIO()):
            self.ctx.go(exclude_task="bc")
        self.assertTrue(os.path.exists(a), "should quit at a")
        self.assertFalse(os.path.exists(b), "should quit at a")
        self.assertFalse(os.path.exists(c), "should quit at a")
        self.assertFalse(os.path.exists(d), "should quit at a")


    def test_go_target(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        self.ctx.add_task("touch [targets[0]]", targets=[a], name="a")
        self.ctx.add_task("touch [targets[0]] [targets[1]]", depends=[a],
                          targets=[b, c], name="bc")
        self.ctx.add_task("touch "+d, depends=[a,b], name="d")
        with capture(stderr=StringIO()):
            self.ctx.go(target=c)
        self.assertTrue(os.path.exists(a), "should quit at bc")
        self.assertTrue(os.path.exists(b), "should quit at bc")
        self.assertTrue(os.path.exists(c), "should quit at bc")
        self.assertFalse(os.path.exists(d), "should quit at bc")


    def test_go_exclude_target(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        self.ctx.add_task("touch [targets[0]]", targets=[a], name="a")
        self.ctx.add_task("touch [targets[0]] [targets[1]]", depends=[a],
                          targets=[b, c], name="bc")
        self.ctx.add_task("touch "+d, depends=[a,b], name="d")
        with capture(stderr=StringIO()):
            self.ctx.go(exclude_target=c)
        self.assertTrue(os.path.exists(a), "should quit at a")
        self.assertFalse(os.path.exists(b), "should quit at a")
        self.assertFalse(os.path.exists(c), "should quit at a")
        self.assertFalse(os.path.exists(d), "should quit at a")


    def test_go_exclude_target_pattern(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        self.ctx.add_task("touch [targets[0]]", targets=[a], name="a")
        self.ctx.add_task("touch [targets[0]] [targets[1]]", depends=[a],
                          targets=[b, c], name="bc")
        self.ctx.add_task("touch "+d, depends=[a,b], name="d")
        with capture(stderr=StringIO()):
            self.ctx.go(exclude_target="*.txt")
        self.assertFalse(os.path.exists(a), "shouldn't execute any tasks")
        self.assertFalse(os.path.exists(b), "shouldn't execute any tasks")
        self.assertFalse(os.path.exists(c), "shouldn't execute any tasks")
        self.assertFalse(os.path.exists(d), "shouldn't execute any tasks")


    def test_go_skip(self):
        outf = os.path.join(self.workdir, "blah.txt")
        self.ctx.add_task("touch [targets[0]]", targets=[outf])
        with capture(stderr=StringIO()):
            self.ctx.go()
        ctime = os.stat(outf).st_ctime
        time.sleep(SLEEPTIME)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(ctime, os.stat(outf).st_ctime)


    def test_go_skip_notargets(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        self.ctx.add_task("touch [targets[0]]", targets=[a])
        self.ctx.add_task("touch [targets[0]] [targets[1]]", targets=[b, c])
        self.ctx.add_task("touch {}".format(d), depends=[a,b])
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(a).st_mtime
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(a).st_mtime)
        os.remove(a)
        time.sleep(SLEEPTIME)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertNotEqual(mtime, os.stat(a).st_mtime)

    def test_go_skip_nothing(self):
        a = os.path.join(self.workdir, "a.txt")
        self.ctx.add_task("touch [targets[0]]", targets=[a])
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(a).st_mtime
        time.sleep(SLEEPTIME)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(a).st_mtime)
        with capture(stderr=StringIO()):
            self.ctx.go(skip_nothing=True)
        self.assertNotEqual(mtime, os.stat(a).st_mtime)

    def test_go_skip_glob(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        globdep = anadama2.tracked.TrackedFilePattern(
            os.path.join(self.workdir, "*.txt")
        )
        out = os.path.join(self.workdir, "out")
        self.ctx.already_exists(globdep)
        for letter in (a,b,c,d):
            anadama2.util.sh("echo {a} > {a}".format(a=letter), shell=True)
        self.ctx.add_task("ls [depends[0]] > [targets[0]]",
                          depends=globdep, targets=out)
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(out).st_mtime
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(out).st_mtime)
        with open(os.path.join(self.workdir, "f.txt"), 'w') as f:
            f.write("hi mom\n")
        time.sleep(SLEEPTIME)
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
        dirdep = anadama2.tracked.TrackedDirectory(self.workdir)
        out = "/tmp/foobaz"
        self.ctx.already_exists(dirdep)
        for letter in (a,b,c,d):
            anadama2.util.sh("echo {a} > {a}".format(a=letter), shell=True)
        self.ctx.add_task("ls [depends[0]] > [targets[0]]",
                          depends=dirdep, targets=out)
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(out).st_mtime
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(out).st_mtime)
        with open(os.path.join(self.workdir, "f.txt"), 'w') as f:
            f.write("hi mom\n")
        time.sleep(SLEEPTIME)
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
        conf = anadama2.tracked.Container(alpha="5", beta=2)
        self.ctx.add_task("echo beta:[depends[0]] > [targets[0]]",
                          depends=conf.beta, targets=a)
        with capture(stderr=StringIO()):
            self.ctx.go()
        mtime = os.stat(a).st_mtime
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(mtime, os.stat(a).st_mtime)
        conf.beta = 7
        time.sleep(SLEEPTIME)
        with capture(stderr=StringIO()):
            self.ctx.go()
        new_mtime = os.stat(a).st_mtime
        self.assertNotEqual(mtime, new_mtime)
        conf.gamma = 10
        time.sleep(SLEEPTIME)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertEqual(new_mtime, os.stat(a).st_mtime)
        self.ctx.add_task("echo beta > [targets[0]]",
                          depends=list(conf.items()), targets=a)
        time.sleep(SLEEPTIME)
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertNotEqual(new_mtime, os.stat(a).st_mtime)


    def test_go_until_task(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        self.ctx.add_task("touch [targets[0]]", targets=[a], name="a")
        self.ctx.add_task("touch [targets[0]] [targets[1]]",
                          targets=[b, c], name="bc", depends=a)
        self.ctx.add_task("touch {}".format(d), depends=[c], targets=d,
                          name="d")
        with capture(stderr=StringIO()):
            self.ctx.go(until_task="bc")
        for f in (a,b,c,):
            self.assertTrue(os.path.isfile(f))
        self.assertFalse(os.path.isfile(d))


    def test_issue36(self):
        ctx = self.ctx
        step1_const = anadama2.tracked.Container(a = 12)
        step1_out = os.path.join(self.workdir, "step1.txt")
        step1_cmd = " ".join(["echo", str(step1_const.a), ">", step1_out])
        ctx.already_exists(anadama2.tracked.TrackedString(step1_cmd))
        step1 = ctx.add_task(step1_cmd,
                             depends=[step1_const.a,
                                      anadama2.tracked.TrackedString(step1_cmd)],
                             targets=[step1_out])

        step2_out = os.path.join(self.workdir, "step2.txt")
        step2_cmd = "; ".join(["p=$(cat " + step1_out + ")",
                               "echo $p > " + step2_out ])
        ctx.already_exists(anadama2.tracked.TrackedString(step2_cmd))

        step2 = ctx.add_task(step2_cmd,
                             depends=[step1_out],
                             targets=[step2_out],
                             name="Step 2")
        with capture(stderr=StringIO()):
            ctx.go()

        step2skipped = False
        class CustomReporter(anadama2.reporters.ConsoleReporter):
            def task_skipped(self, task_no, *args, **kwargs):
                if task_no == step2.task_no:
                    step2skipped = True
                return super(CustomReporter, self).task_skipped(
                    task_no, *args, **kwargs)
            def task_running(self, task_no):
                pass

        step1_const.a = 10
        with capture(stderr=StringIO()):
            ctx.go(reporter=CustomReporter(ctx))

        self.assertFalse(step2skipped,
                         "Shouldn't skip step 2; parent dep changed")


    def test_print_within_function_action(self):
        stderr_msg = six.u("".join([chr(random.randint(32, 126)) for _ in range(10)]))
        stdout_msg = six.u("".join([chr(random.randint(32, 126)) for _ in range(10)]))
        def printer(task):
            sys.stderr.write(stderr_msg+"\n")
            sys.stdout.write(stdout_msg+"\n")

        t1 = self.ctx.add_task(printer)

        out, err = StringIO(), StringIO()
        with capture(stdout=out, stderr=err):
            self.ctx.go()

        self.assertNotIn(t1.task_no, self.ctx.failed_tasks)
        self.assertIn(stdout_msg, out.getvalue())
        self.assertIn(stderr_msg, err.getvalue())
       
    def test__sugar_list(self):
        rc = anadama2.workflow
        self.assertEqual([5], rc.sugar_list(5))
        self.assertEqual(["blah"], rc.sugar_list("blah"))
        it = iter(list(range(5)))
        self.assertIs(it, rc.sugar_list(it))
        t = anadama2.Task("dummy task", [""], [], [], 0, True, [""], None, False)
        self.assertEqual([t], rc.sugar_list(t))
        self.assertEqual((5,), rc.sugar_list((5,)))


    def test_add_task_custom_dependency(self):
        class CustomDependency(anadama2.tracked.Base):
            @staticmethod
            def key(the_key):
                return str(the_key)

            def compare(self):
                self.compared = True
                yield str(self.name)

            def init(self, key):
                self.initialized = True
                self.compared = False

        d = CustomDependency("blah")
        self.ctx.add_task(anadama2.util.noop, targets=d)
        self.assertTrue(d.initialized)
        self.assertFalse(d.compared)
        with capture(stdout=StringIO(), stderr=StringIO()):
            self.ctx.go()
        self.assertTrue(d.compared)


    def test_TaskContainer(self):
        a,b,c,d = [os.path.join(self.workdir, letter+".txt")
                   for letter in ("a", "b", "c", "d")]
        t1 = self.ctx.add_task("touch [targets[0]]", targets=[a], name="a")
        t2 = self.ctx.add_task("touch [targets[0]] [targets[1]]",
                               targets=[b, c], name="bc")
        t3 = self.ctx.add_task("touch {}".format(d), depends=[a,b], name="d")
        self.assertTrue(isinstance(self.ctx.tasks, anadama2.taskcontainer.TaskContainer))
        self.assertIs(self.ctx.tasks[t1.task_no], t1)
        self.assertIs(self.ctx.tasks[t2.name], t2)

    def test_autopreexist(self):
        a = os.path.join(self.workdir, "a.txt")
        b = os.path.join(self.workdir, "b.txt")
        with self.assertRaises(KeyError):
            self.ctx.add_task("cat [depends[0]] > [targets[0]]", 
                              depends=a, targets=b, name="shouldfail")
        self.assertEqual(len(self.ctx.tasks), 0, "should remove offending task")
        open(a, 'w').close()
        self.ctx.add_task("cat [depends[0]] > [targets[0]]", 
                          depends=a, targets=b, name="shouldntfail")
        self.assertEqual(len(self.ctx.tasks), 2)
        self.assertIs(self.ctx.tasks[1].actions[0], anadama2.util.noop)
        self.assertEqual(str(self.ctx.tasks[1].targets[0]), a)
        self.assertEqual(str(self.ctx.tasks[0].depends[0]), a)
        self.assertEqual(str(self.ctx.tasks[0].targets[0]), b)
        
    def test_autopreexist_strict(self):
        a = os.path.join(self.workdir, "a.txt")
        b = os.path.join(self.workdir, "b.txt")
        self.ctx.strict = True
        with self.assertRaises(KeyError):
            self.ctx.add_task("cat [depends[0]] > [targets[0]]", 
                              depends=a, targets=b, name="shouldfail")
        self.assertEqual(len(self.ctx.tasks), 0, "should remove offending task")
        open(a, 'w').close()
        with self.assertRaises(KeyError):
            self.ctx.add_task("cat [depends[0]] > [targets[0]]", 
                              depends=a, targets=b, name="shouldntfail")
        self.assertEqual(len(self.ctx.tasks), 0)


        
if __name__ == "__main__":
    unittest.main()
