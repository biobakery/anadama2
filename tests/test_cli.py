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
        c = anadama2.cli.Configuration()
        self.assertIs(type(c), anadama2.cli.Configuration)
        self.assertTrue(hasattr(c, "description"))
        self.assertTrue(hasattr(c, "version"))
        self.assertTrue(hasattr(c, "args"))

    def test_Configuration_add(self):
        c = anadama2.cli.Configuration()
        ret = c.add("foo", desc="it's a foo", default="bar", type="str")
        self.assertIs(ret, c)
        self.assertTrue("foo" in c._directives)
        self.assertTrue(isinstance(c._directives['foo'], optparse.Option))
        o = c._directives['foo']
        self.assertEqual(o._short_opts[0], "-f")
        self.assertEqual(o._long_opts[0], "--foo")
        self.assertEqual(o.help, "it's a foo")
        self.assertEqual(o.default, "bar")
        self.assertEqual(o.type, "string")

    def test_Configuration_defaults(self):
        c = anadama2.cli.Configuration(defaults=True)
        self.assertGreater(len(c._directives), 1)
        self.assertGreater(len(c._shorts), 1)

    def test_Configuration_ask_user(self):
        c = anadama2.cli.Configuration(defaults=True)
        c.ask_user(argv=["--until-task", "blarg"])
        self.assertTrue(hasattr(c, "until_task"))
        self.assertEqual(c.until_task, "blarg")
        self.assertTrue(c._user_asked)

    def test_Configureation_directory(self):
        c = anadama2.cli.Configuration().add(
            "input", type="dir"
        ).ask_user(argv=["--input", self.workdir])
        self.assertIs(type(c.input), anadama2.util.Directory)
        self.assertEqual(c.input.name, self.workdir)
        
    
        
if __name__ == "__main__":
    unittest.main()
