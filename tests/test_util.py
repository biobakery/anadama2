# -*- coding:utf-8 -*-
import os
import shutil
import unittest

import six

import anadama2.util
from anadama2.util import fname

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
        first = anadama2.util._adler32(f)
        with open(f, 'w+') as _f:
            _f.write(six.u("blah blah\n"))
        second = anadama2.util._adler32(f)
        open(f, 'w').close()
        third = anadama2.util._adler32(f)
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

    def test_kebab(self):
        self.assertEqual(anadama2.util.kebab("drunken     sailor"), "drunken-sailor")
        self.assertEqual(anadama2.util.kebab("drunken-s"+six.unichr(228)+"ilor"), "drunken-sailor")
        self.assertEqual(anadama2.util.kebab("drunken-209sailor"), "drunken-sailor")
        self.assertEqual(anadama2.util.kebab("drunken-,.!@#$%^&*()sailor"), "drunken-sailor")
        self.assertEqual(anadama2.util.kebab("drunken-_+|\\sailor"), "drunken-sailor")
        self.assertEqual(anadama2.util.kebab("drunken-[]}{'sailor"), "drunken-sailor")
        self.assertEqual(anadama2.util.kebab('drunken-"sailor'), "drunken-sailor")
        self.assertEqual(anadama2.util.kebab('drunken-~`><sailor'), "drunken-sailor")


    def test_Directory(self):
        touch = lambda f: open(f, 'w').close()
        path = self.workdir+"/input"
        d = anadama2.util.Directory(path)
        self.assertFalse(d.exists())
        d.create()
        self.assertTrue(d.exists())
        for c in "abcd":
            touch(d.name+"/"+c+".txt")
            touch(d.name+"/"+c+".tsv")
        
        self.assertEqual(len(d.files()), 8)
        self.assertEqual(len(d.files("*.txt")), 4)
        self.assertEqual(len(d.files("*.tsv")), 4)
        self.assertEqual(d.name, path)
        
        
if __name__ == "__main__":
    unittest.main()
