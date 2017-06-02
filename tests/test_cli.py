# -*- coding: utf-8 -*-
import os
import shutil
import unittest
import optparse

import anadama2.cli
import anadama2.util


class TestCli(unittest.TestCase):

    def setUp(self):
        self.workdir = "/tmp/anadama_testdir"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)

    def tearDown(self):
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)

    def test_Configuraion_instantiate(self):
        c = anadama2.cli.Configuration(prompt_user=False)
        self.assertIs(type(c), anadama2.cli.Configuration)
        self.assertTrue(hasattr(c, "description"))
        self.assertTrue(hasattr(c, "version"))

    def test_Configuration_add(self):
        c = anadama2.cli.Configuration(defaults=False,prompt_user=False)
        ret = c.add("foo", desc="it's a foo", default="bar", type="str", short="-f")
        self.assertIs(ret, c)
        self.assertTrue("foo" in c._user_arguments)
        o = c._user_arguments['foo']
        self.assertEqual(o.keywords["help"], "it's a foo\n[default: %(default)s]")
        self.assertEqual(o.keywords["default"], "bar")
        self.assertEqual(o.keywords["type"], "str")

    def test_Configuration_remove(self):
        c = anadama2.cli.Configuration(prompt_user=False)
        c.remove("output")
        self.assertFalse("output" in c._arguments)
        c.ask_user()
        self.assertIs(c.get("output"), None)

    def test_Configuration_defaults(self):
        c = anadama2.cli.Configuration(defaults=True)
        self.assertGreater(len(c._arguments), 1)
        self.assertGreater(len(c._shorts), 1)

    def test_Configuration_ask_user(self):
        c = anadama2.cli.Configuration(defaults=True,remove_options=["output"])
        c.ask_user(argv=["--until-task", "blarg"])
        self.assertTrue(hasattr(c, "until_task"))
        self.assertEqual(c.until_task, "blarg")
        self.assertTrue(c._user_asked)
        
    
        
if __name__ == "__main__":
    unittest.main()
