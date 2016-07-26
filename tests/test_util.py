import os
import shutil
import unittest

import anadama.util
from anadama.util import fname

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


    def test_fname(self):
        indir = self.workdir+"/input"
        infile = indir+"/data.txt"
        outdir = self.workdir+"/output"
        self.assertEqual(fname.mangle(infile, dir=outdir),
                         outdir+"/data.txt")
        self.assertEqual(fname.mangle(infile, ext="tsv"),
                         indir+"/data.tsv")
        self.assertEqual(fname.mangle(infile, dir=outdir, ext=""),
                         outdir+"/data")
        self.assertEqual(fname.mangle(infile, tag="clean"),
                         indir+"/data_clean.txt")
        self.assertEqual(fname.mangle(infile, dir=outdir+"/data", tag="special", ext="log"),
                         outdir+"/data/data_special.log")

    def test_addtag(self):
        self.assertEqual(fname.addtag("/shares/hiibroad/data/humann2_medclean/input/Batch15_WGS_S/6819351/processed/6819351_2.fastq.bz2", "special"),
                         "/shares/hiibroad/data/humann2_medclean/input/Batch15_WGS_S/6819351/processed/6819351_2_special.fastq.bz2")


        
if __name__ == "__main__":
    unittest.main()
