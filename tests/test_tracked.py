# -*- coding: utf-8 -*-
import os
import shutil
import unittest

import anadama2
import anadama2.tracked
import anadama2.backends


class TestTracked(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ[anadama2.backends.ENV_VAR] = "/tmp/anadama_testdb"
    

    def setUp(self):
        self.workdir = "/tmp/anadama_testdir"
        self.db_dir = "/tmp/anadama_testdb"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)
        self.be = anadama2.backends.default(self.db_dir)


    def tearDown(self):
        self.be.close()
        del self.be
        anadama2.backends._default_backend = None
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        if os.path.isdir(self.db_dir):
            shutil.rmtree(self.db_dir)

        
    def test_any_different(self):
        f = anadama2.tracked.TrackedFile(os.path.join(self.workdir, "blah.txt"))
        open(str(f), 'w').close()
        self.assertTrue(anadama2.tracked.any_different([f], self.be),
                        ("The backend hasn't saved this key before, so there "
                         "should be a difference between None and the "
                         "current dep"))
        compare = list(f.compare())
        self.be.save([f.name], [compare])
        self.assertFalse(anadama2.tracked.any_different([f], self.be),
                         ("The backend has seen this dep before and the "
                          "dep hasn't changed, so there should be no "
                          "difference"))
        
    def test_auto(self):
        t = anadama2.Task("dummy task", [""], [], [], 0, True)
        self.assertIsInstance(anadama2.tracked.auto(t), anadama2.Task)
        n = "/tmp/foobaz"
        self.assertIsInstance(anadama2.tracked.auto(n), anadama2.tracked.TrackedFile)
        d = "/tmp/"
        self.assertIsInstance(anadama2.tracked.auto(d), anadama2.tracked.TrackedDirectory)
        self.assertIsInstance(anadama2.tracked.auto(lambda *a, **kw: None),
                              anadama2.tracked.TrackedFunction)
        self.assertIsInstance(anadama2.tracked.TrackedString("garglefwonk"),
                              anadama2.tracked.TrackedString)
        with self.assertRaises(ValueError):
            anadama2.tracked.auto(["a", 5])
        
