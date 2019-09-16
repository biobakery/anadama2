# -*- coding: utf-8 -*-
import os
import sys
import shutil
import unittest
import subprocess

import anadama2
import anadama2.tracked
from anadama2 import picklerunner
from anadama2.runners import TaskResult


class TestPicklerunner(unittest.TestCase):

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
        cfg = anadama2.cli.Configuration(prompt_user=False).add("output", type="dir", default=self.workdir)
        self.ctx = anadama2.workflow.Workflow(vars=cfg)
        self.stub_result=TaskResult(1,None,[],[])


    def tearDown(self):
        if self.ctx._backend:
            self.ctx._backend.close()
            del self.ctx._backend
            self.ctx._backend = None
            anadama2.backends._default_backend = None
            
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)

    def test_write(self):
        outf = os.path.join(self.workdir, "hosts")
        t = self.ctx.add_task("ls -alh /etc/hosts > [targets[0]]",
                              targets=outf)
        s = picklerunner.PickleScript(t,self.workdir,"test")
        new_task = s.create_task()
        self.assertTrue(os.path.exists(s.script_file))
        proc = subprocess.Popen([sys.executable,s.script_file], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(proc.returncode, 0)

    def test_decode(self):
        outf = os.path.join(self.workdir, "hosts")
        t = self.ctx.add_task("ls -alh /etc/hosts > [targets[0]]",
                              targets=outf)
        s = picklerunner.PickleScript(t,self.workdir,"test")
        new_task = s.create_task()
        self.assertTrue(os.path.exists(s.script_file))
        self.assertFalse(os.path.exists(outf))
        proc = subprocess.Popen([sys.executable,s.script_file], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(proc.returncode, 0)
        self.assertTrue(os.path.exists(s.script_file))
        result = s.result(self.stub_result)
        self.assertFalse(bool(result.error))
        self.assertEqual(len(result.dep_keys), 1)
        compares = list(anadama2.tracked.HugeTrackedFile(outf).compare())
        self.assertEqual(len(compares), len(result.dep_compares[0]))
        for a, b in zip(result.dep_compares[0], compares):
            self.assertEqual(a, b, "compare() not the same")
        

    def test_decode_fail(self):
        outf = os.path.join(self.workdir, "hosts")
        t = self.ctx.add_task("ls -alh /etc/hosts > [targets[0]]; exit 1",
                              targets=outf)
        s = picklerunner.PickleScript(t,self.workdir,"test")
        new_task = s.create_task()
        self.assertTrue(os.path.exists(s.script_file))
        self.assertFalse(os.path.exists(outf))
        proc = subprocess.Popen([sys.executable,s.script_file], 
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(proc.returncode, 0)
        self.assertTrue(os.path.exists(outf))
        result = s.result(self.stub_result)
        self.assertTrue(bool(result.error))
        self.assertIn("ShellException", result.error)
        
        
if __name__ == "__main__":
    unittest.main()

