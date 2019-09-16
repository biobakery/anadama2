# -*- coding: utf-8 -*-
import os
import shutil
import unittest

import anadama2
import anadama2.tracked
import anadama2.workflow
import anadama2.util
from anadama2.util import noop
import anadama2.backends
import anadama2.taskcontainer


SLEEPTIME = os.environ.get("ANADAMA_SLEEP_TIME", "0.01")
SLEEPTIME=float(SLEEPTIME)

class TestStructure(unittest.TestCase):

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


    def tearDown(self):
        if self.ctx._backend:
            self.ctx._backend.close()
            del self.ctx._backend
            self.ctx._backend = None
            anadama2.backends._default_backend = None
            
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        
    def test_linear(self):
        a,b,c = map(str, list(range(3)))
        self.ctx.add_task(noop, targets=a,            name="a")
        self.ctx.add_task(noop, targets=b, depends=a, name="b")
        self.ctx.add_task(noop, targets=c, depends=b, name="c")
        self.assertEqual(len(self.ctx.tasks), 3)
        nodes = set(self.ctx.dag.nodes())
        for i in range(3):
            self.assertIn(i, nodes)
        self.assertEqual(self.ctx.dag.successors(0), [1])
        self.assertEqual(self.ctx.dag.successors(1), [2])
        self.assertEqual(self.ctx.dag.successors(2), [])
        self.assertEqual(self.ctx.dag.predecessors(0), [])
        self.assertEqual(self.ctx.dag.predecessors(1), [0])
        self.assertEqual(self.ctx.dag.predecessors(2), [1])
        self.assertEqual(self.ctx.tasks['a'], self.ctx.tasks[0])
        self.assertEqual(self.ctx.tasks['b'], self.ctx.tasks[1])
        self.assertEqual(self.ctx.tasks['c'], self.ctx.tasks[2])

    def test_expand(self):
        a,b,c,d = map(str, list(range(4)))
        self.ctx.add_task(noop, targets=a,            name="a")
        self.ctx.add_task(noop, targets=b, depends=a, name="b")
        self.ctx.add_task(noop, targets=c, depends=a, name="c")
        self.ctx.add_task(noop, targets=d, depends=a, name="d")
        self.assertEqual(len(self.ctx.tasks), 4)
        nodes = set(self.ctx.dag.nodes())
        for i in range(4):
            self.assertIn(i, nodes)
        self.assertEqual(self.ctx.dag.successors(0), [1,2,3])
        self.assertEqual(self.ctx.dag.successors(1), [])
        self.assertEqual(self.ctx.dag.successors(2), [])
        self.assertEqual(self.ctx.dag.successors(3), [])
        self.assertEqual(self.ctx.dag.predecessors(0), [])
        self.assertEqual(self.ctx.dag.predecessors(1), [0])
        self.assertEqual(self.ctx.dag.predecessors(2), [0])
        self.assertEqual(self.ctx.dag.predecessors(3), [0])
        self.assertEqual(self.ctx.tasks['a'], self.ctx.tasks[0])
        self.assertEqual(self.ctx.tasks['b'], self.ctx.tasks[1])
        self.assertEqual(self.ctx.tasks['c'], self.ctx.tasks[2])
        self.assertEqual(self.ctx.tasks['d'], self.ctx.tasks[3])

    def test_reduce(self):
        a,b,c,d = map(str, list(range(4)))
        self.ctx.add_task(noop, targets=a,                  name="a")
        self.ctx.add_task(noop, targets=b,                  name="b")
        self.ctx.add_task(noop, targets=c,                  name="c")
        self.ctx.add_task(noop, targets=d, depends=[a,b,c], name="d")
        self.assertEqual(len(self.ctx.tasks), 4)
        nodes = set(self.ctx.dag.nodes())
        for i in range(4):
            self.assertIn(i, nodes)
        self.assertEqual(self.ctx.dag.successors(0), [3])
        self.assertEqual(self.ctx.dag.successors(1), [3])
        self.assertEqual(self.ctx.dag.successors(2), [3])
        self.assertEqual(self.ctx.dag.successors(3), [])
        self.assertEqual(self.ctx.dag.predecessors(0), [])
        self.assertEqual(self.ctx.dag.predecessors(1), [])
        self.assertEqual(self.ctx.dag.predecessors(2), [])
        self.assertEqual(self.ctx.dag.predecessors(3), [0,1,2])
        self.assertEqual(self.ctx.tasks['a'], self.ctx.tasks[0])
        self.assertEqual(self.ctx.tasks['b'], self.ctx.tasks[1])
        self.assertEqual(self.ctx.tasks['c'], self.ctx.tasks[2])
        self.assertEqual(self.ctx.tasks['d'], self.ctx.tasks[3])


    def test_expandreduce(self):
        a,b,c,d,e = map(str, list(range(5)))
        self.ctx.add_task(noop, targets=a,                  name="a")
        self.ctx.add_task(noop, targets=b, depends=a,       name="b")
        self.ctx.add_task(noop, targets=c, depends=a,       name="c")
        self.ctx.add_task(noop, targets=d, depends=a,       name="d")
        self.ctx.add_task(noop, targets=e, depends=[b,c,d], name="e")
        
        self.assertEqual(len(self.ctx.tasks), 5)
        nodes = set(self.ctx.dag.nodes())
        for i in range(5):
            self.assertIn(i, nodes)
        self.assertEqual(self.ctx.dag.successors(0), [1,2,3])
        self.assertEqual(self.ctx.dag.successors(1), [4])
        self.assertEqual(self.ctx.dag.successors(2), [4])
        self.assertEqual(self.ctx.dag.successors(3), [4])
        self.assertEqual(self.ctx.dag.successors(4), [])
        self.assertEqual(self.ctx.dag.predecessors(0), [])
        self.assertEqual(self.ctx.dag.predecessors(1), [0])
        self.assertEqual(self.ctx.dag.predecessors(2), [0])
        self.assertEqual(self.ctx.dag.predecessors(3), [0])
        self.assertEqual(self.ctx.dag.predecessors(4), [1,2,3])
        self.assertEqual(self.ctx.tasks['a'], self.ctx.tasks[0])
        self.assertEqual(self.ctx.tasks['b'], self.ctx.tasks[1])
        self.assertEqual(self.ctx.tasks['c'], self.ctx.tasks[2])
        self.assertEqual(self.ctx.tasks['d'], self.ctx.tasks[3])
        self.assertEqual(self.ctx.tasks['e'], self.ctx.tasks[4])


if __name__ == "__main__":
    unittest.main()
