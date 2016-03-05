import unittest2

import networkx

import anadama
import anadama.deps
import anadama.runcontext

class TestRunContext(unittest2.TestCase):

    def setUp(self):
        self.ctx = anadama.runcontext.RunContext()

    def test_hasattributes(self):
        self.assertTrue(issubclass(self.ctx.dag,
                                   networkx.classes.digraph.DiGraph))
        self.assertIs(type(self.ctx.tasks), list)
        self.assertTrue(hasattr(self.ctx, "task_counter"))

    def test_do_simple(self):
        t1 = self.ctx.do("echo true", track_cmd=False, track_binaries=False)
        self.assertTrue(issubclass(t1, anadama.Task))
        self.assertIs(t1, self.ctx.tasks[0])
        self.assertEqual(len(t1.depends), 0)
        self.assertEqual(len(t1.targets), 0)
        self.assertEqual(len(t1.actions), 1)

    def test_do_track_cmd(self):
        t1 = self.ctx.do("echo true", track_binaries=False)
        self.assertEqual(len(t1.depends), 1)
        self.assertEqual(len(t1.targets), 0)
        self.assertEqual(len(t1.actions), 1)
        self.assertTrue(issubclass(t1.depends[0],
                                   anadama.deps.StringDependency))
        
    def test_do_track_binaries(self):
        t1 = self.ctx.do("echo true", track_cmd=False)
        self.assertEqual(len(t1.depends), 2)
        self.assertEqual(len(t1.targets), 0)
        self.assertEqual(len(t1.actions), 1)
        self.assertTrue(issubclass(t1.depends[0],
                                   anadama.deps.ExecutableDependency))
        self.assertTrue(issubclass(t1.depends[1],
                                   anadama.deps.ExecutableDependency))
        
        

if __name__ == "__main__":
    unittest2.main()
