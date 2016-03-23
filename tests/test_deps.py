import os
import shutil
import inspect
import unittest

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
        self.assertIsInstance(anadama.deps.auto(t), anadama.deps.TaskDependency)
        n = "/tmp/foobaz"
        self.assertIsInstance(anadama.deps.auto(n), anadama.deps.FileDependency)
        self.assertIsInstance(anadama.deps.auto(lambda *a, **kw: None),
                              anadama.deps.FunctionDependency)
        self.assertIsInstance(anadama.deps.StringDependency("garglefwonk"),
                              anadama.deps.StringDependency)
        with self.assertRaises(ValueError):
            anadama.deps.auto(["a", 5])
        


    def test_CompareCache(self):
        f = anadama.deps.FileDependency(
            os.path.join(self.workdir, "compare_cache.txt"))
        f.n_compares = 0
        prev_compare = f.compare
        def _compare(*args, **kwargs):
            f.n_compares += 1
            return prev_compare(*args, **kwargs)
        f.compare = _compare
        with open(str(f), 'w') as _f:
            print >> _f, "blah blah"
        self.assertEqual(f.n_compares, 0, "no compares should be done yet")
        cache = anadama.deps.CompareCache()
        compare1 = cache(f)
        self.assertTrue(inspect.isgenerator(compare1),
                        "compare caches should return generators")
        c1 = list(compare1)
        self.assertEqual(f.n_compares, 1,
                         ("The compare_cache should do compare() just once "
                          "because it hasn't seen this dep before"))
        self.assertTrue(f._key in cache.c,
                        ("The compare cache should insert the FileDependency "
                         "._key in its internal cache"))
        compare2 = cache(f)
        c2 = list(compare2)
        self.assertTrue(inspect.isgenerator(compare2),
                        ("compare caches should return generators even when "
                         "using cached results"))
        self.assertEqual(c1, c2)
        self.assertEqual(f.n_compares, 1,
                         ("n_compares shouldn't go up because the base "
                          "compare isn't executed"))
        
        cache.clear()
        self.assertTrue(f._key not in cache.c,
                        ("The compare cache should remove the FileDependency "
                         "._key from its internal cache upon .clear()"))
