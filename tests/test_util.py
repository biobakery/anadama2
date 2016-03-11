import os
import shutil
import unittest

import anadama.util


class TestUtil(unittest.TestCase):

    def setUp(self):
        self.workdir = "/tmp/anadama_testdir"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)


    def tearDown(self):
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)


    def test__adler32(self):
        f = os.path.join(self.workdir, "test.txt")
        open(f, 'w').close()
        first = anadama.util._adler32(f)
        with open(f, 'w+') as _f:
            print >> _f, "blah blah"
        second = anadama.util._adler32(f)
        open(f, 'w').close()
        third = anadama.util._adler32(f)
        self.assertNotEqual(first, second)
        self.assertNotEqual(second, third)
        self.assertEqual(first, third)
