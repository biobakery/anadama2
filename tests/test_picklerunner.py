import os
import shutil
import unittest
import subprocess

import anadama
import anadama.deps
from anadama import picklerunner


class TestPicklerunner(unittest.TestCase):

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
        

    def test_PickleScript(self):
        outf = os.path.join(self.workdir, "hosts")
        t = self.ctx.add_task("ls -alh /etc/hosts > {targets[0]}",
                              targets=outf)
        s = picklerunner.PickleScript(t)
        data = s.render()
        fname = os.path.join(self.workdir, "script.py")
        s.save(path=fname)
        self.assertTrue(os.path.exists(fname))
        with open(fname, 'r') as f:
            self.assertEqual(f.read(), data)


    def test_tmp(self):
        outf = os.path.join(self.workdir, "hosts")
        t = self.ctx.add_task("ls -alh /etc/hosts > {targets[0]}",
                              targets=outf)
        s = picklerunner.tmp(t)
        self.assertTrue(os.path.exists(s.path))
        proc = subprocess.Popen([s.path, "-r", "-p"], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(proc.returncode, 0)
        self.assertFalse(os.path.exists(s.path))
        self.assertTrue(os.path.exists(outf))
        self.assertIn(picklerunner.PICKLE_KEY, out)

    def test_decode(self):
        outf = os.path.join(self.workdir, "hosts")
        t = self.ctx.add_task("ls -alh /etc/hosts > {targets[0]}",
                              targets=outf)
        s = picklerunner.tmp(t)
        self.assertTrue(os.path.exists(s.path))
        self.assertFalse(os.path.exists(outf))
        proc = subprocess.Popen([s.path], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(proc.returncode, 0)
        self.assertTrue(os.path.exists(s.path))
        self.assertTrue(os.path.exists(outf))
        with self.assertRaises(ValueError):
            picklerunner.decode(out)
        proc = subprocess.Popen([s.path, "-p"], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(proc.returncode, 0)
        self.assertTrue(os.path.exists(s.path))
        result = picklerunner.decode(out)
        self.assertFalse(bool(result.error))
        self.assertEqual(len(result.dep_keys), 1)
        compares = list(anadama.deps.FileDependency(outf).compare())
        self.assertEqual(len(compares), len(result.dep_compares[0]))
        for a, b in zip(result.dep_compares[0], compares):
            self.assertEqual(a, b, "compare() not the same")
        

    def test_decode_fail(self):
        outf = os.path.join(self.workdir, "hosts")
        t = self.ctx.add_task("ls -alh /etc/hosts > {targets[0]}; exit 1",
                              targets=outf)
        s = picklerunner.tmp(t)
        self.assertTrue(os.path.exists(s.path))
        self.assertFalse(os.path.exists(outf))
        proc = subprocess.Popen([s.path, "-p"], 
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(proc.returncode, 0)
        self.assertTrue(os.path.exists(s.path))
        self.assertTrue(os.path.exists(outf))
        result = picklerunner.decode(out)
        self.assertTrue(bool(result.error))
        self.assertIn("ShellException", result.error)
        


