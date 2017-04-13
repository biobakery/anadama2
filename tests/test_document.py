# -*- coding: utf-8 -*-
import os
import shutil
import unittest
import optparse

import anadama2.document


class TestPweaveDocument(unittest.TestCase):

    def test_filter_zero_rows(self):
        doc = anadama2.document.PweaveDocument()
        names=["s1","s2","s3"]
        data=[[0,0,1],[0,0,0],[1,0,0]]

        filtered_names, filtered_data = doc.filter_zero_rows(names,data)
        
        self.assertEqual(filtered_names,["s1","s3"])
        for x,y in zip(filtered_data, [[0,0,1],[1,0,0]]):
            self.assertListEqual(x,y)
        
    def test_filter_zero_rows_no_zeros(self):
        doc = anadama2.document.PweaveDocument()
        names=["s1","s2","s3"]
        data=[[0,0,1],[0,1,0],[1,0,0]]

        filtered_names, filtered_data = doc.filter_zero_rows(names,data)
        
        self.assertEqual(filtered_names,["s1","s2","s3"])
        for x,y in zip(filtered_data, [[0,0,1],[0,1,0],[1,0,0]]):
            self.assertListEqual(x,y)
        
    def test_filter_zero_columns(self):
        doc = anadama2.document.PweaveDocument()
        names=["s1","s2","s3"]
        data=[[0,0,1],[0,0,0],[1,0,0]]

        filtered_names, filtered_data = doc.filter_zero_columns(names,data)
        
        self.assertEqual(filtered_names,["s1","s3"])
        for x,y in zip(filtered_data, [[0,1],[0,0],[1,0]]):
            self.assertListEqual(x,y)
        
    def test_filter_zero_columns_no_zeros(self):
        doc = anadama2.document.PweaveDocument()
        names=["s1","s2","s3"]
        data=[[0,0,1],[0,1,0],[1,0,0]]

        filtered_names, filtered_data = doc.filter_zero_columns(names,data)
        
        self.assertEqual(filtered_names,["s1","s2","s3"])
        for x,y in zip(filtered_data,[[0,0,1],[0,1,0],[1,0,0]]):
            self.assertListEqual(x,y)
        
    
        
if __name__ == "__main__":
    unittest.main()
