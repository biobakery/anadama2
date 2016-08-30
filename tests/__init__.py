# -*- coding: utf-8 -*-
import os
import unittest

here = os.path.dirname(os.path.realpath(__file__))

def test_suite():
    return unittest.TestSuite([
        unittest.TestLoader().discover(here, pattern="test_*.py")
    ])
    
