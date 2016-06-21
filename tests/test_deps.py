import os
import shutil
import inspect
import unittest

import anadama
import anadama.deps
import anadama.backends


class TestDeps(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ[anadama.backends.ENV_VAR] = "/tmp/anadama_testdb"
    

    def setUp(self):
        self.be = anadama.backends.default()        
        self.workdir = "/tmp/anadama_testdir"
        self.db_dir = "/tmp/anadama_testdb"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)


    def tearDown(self):
        self.be.close()
        del self.be
        anadama.backends._default_backend = None
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        if os.path.isdir(self.db_dir):
            shutil.rmtree(self.db_dir)

        
    def test_any_different(self):
        f = anadama.deps.FileDependency(os.path.join(self.workdir, "blah.txt"))
        open(str(f), 'w').close()
        self.assertTrue(anadama.deps.any_different([f], self.be),
                        ("The backend hasn't saved this key before, so there "
                         "should be a difference between None and the "
                         "current dep"))
        compare = list(f.compare())
        self.be.save([f._key], [compare])
        self.assertFalse(anadama.deps.any_different([f], self.be),
                         ("The backend has seen this dep before and the "
                          "dep hasn't changed, so there should be no "
                          "difference"))
        
    def test_auto(self):
        t = anadama.Task("dummy task", [], [], [], 0)
        self.assertIsInstance(anadama.deps.auto(t), anadama.Task)
        n = "/tmp/foobaz"
        self.assertIsInstance(anadama.deps.auto(n), anadama.deps.FileDependency)
        d = "/tmp/"
        self.assertIsInstance(anadama.deps.auto(d), anadama.deps.DirectoryDependency)
        self.assertIsInstance(anadama.deps.auto(lambda *a, **kw: None),
                              anadama.deps.FunctionDependency)
        self.assertIsInstance(anadama.deps.StringDependency("garglefwonk"),
                              anadama.deps.StringDependency)
        with self.assertRaises(ValueError):
            anadama.deps.auto(["a", 5])
        
