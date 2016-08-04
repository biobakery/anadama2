import os
import shutil
import unittest

import anadama.helpers


class TestHelpers(unittest.TestCase):

    def setUp(self):
        self.workdir = "/tmp/anadama_testdir"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)


    def tearDown(self):
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)


    def test_system(self):
        f = os.path.join(self.workdir, "test.txt")
        anadama.helpers.system(["touch", f])(None)
        self.assertTrue(os.path.isfile(f))
        anadama.helpers.system(["rm", f])(None)
        self.assertFalse(os.path.isfile(f))
        anadama.helpers.system(["echo", "hi"], stdout=f)(None)
        s = os.stat(f).st_size
        self.assertGreater(s, 0)
        anadama.helpers.system(["echo", "hi"], stdout=f)(None)
        self.assertEqual(os.stat(f).st_size, 2*s)
        anadama.helpers.system(["echo", "hi"], stdout_clobber=f)(None)
        self.assertEqual(os.stat(f).st_size, s)
        e = os.path.join(self.workdir, "err.txt")
        anadama.helpers.system(["python", '-c' 'import sys; sys.stderr.write("foobaz");'],
                               stderr=e)(None)
        s = os.stat(e).st_size
        self.assertGreater(s, 0)
        anadama.helpers.system(["python", '-c' 'import sys; sys.stderr.write("foobaz");'],
                               stderr=e)(None)
        self.assertEqual(os.stat(e).st_size, 2*s)
        anadama.helpers.system(["python", '-c' 'import sys; sys.stderr.write("foobaz");'],
                               stderr_clobber=e)(None)
        self.assertEqual(os.stat(e).st_size, s)
        anadama.helpers.system(["python", '-c' 'import sys; sys.stderr.write(sys.stdin.read());'],
                               stdout_clobber=f, stdin=e)(None)
        self.assertEqual(os.stat(e).st_size, s)
        

if __name__ == '__main__':
    unittest.main()
