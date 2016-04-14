import os
import shutil
import unittest
from cStringIO import StringIO

import anadama
from anadama.runcontext.grid import SLURMMixin
from anadama.util import find_on_path

from util import capture

class SlurmContext(anadama.RunContext, SLURMMixin):
    pass

srun_exists = bool(find_on_path("srun"))

PARTITION = os.environ.get("ANASLURM_TEST_PARTITION", "general")
TMPDIR = os.environ.get("ANASLURM_TMPDIR", "tmp")

class TestPicklerunner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ[anadama.backends.ENV_VAR] = "/tmp/anadamatest"
    
    @classmethod
    def tearDownClass(cls):
        if os.path.isdir("/tmp/anadamatest"):
            shutil.rmtree("/tmp/anadamatest")


    def setUp(self):
        self.ctx = SlurmContext(PARTITION, TMPDIR)
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
        

    @unittest.skipUnless(srun_exists, "requires srun")
    def test_slurm_do(self):
        self.ctx.slurm_do("echo true > @{true.txt}", time=5, mem=50, cores=1)
        self.assertFalse(os.path.exists("true.txt"))
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertTrue(os.path.exists("true.txt"))
        os.remove("true.txt")
        

