# -*- coding: utf-8 -*-
import os
import shutil
import unittest

from anadama2.reporters import LoggerReporter

class TestReporters(unittest.TestCase):
    

    def setUp(self):
        self.workdir = "/tmp/anadama_testdir"
        # create the work directory
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)
        
        # create a demo log file
        self.demo_log_text="""
            2017-05-25 17:22:26,393    LoggerReporter    started    INFO: Beginning AnADAMA run with 437 tasks.
            2017-05-25 17:22:26,399    LoggerReporter    started    INFO: Workflow configuration options
            2017-05-25 17:22:26,399    LoggerReporter    started    INFO: input_metatranscriptome = input
            2017-05-25 17:22:26,400    LoggerReporter    started    INFO: grid_benchmark = on
            2017-05-25 17:22:26,400    LoggerReporter    started    INFO: grid_partition = serial_requeue,general,240
            2017-05-25 17:22:26,400    LoggerReporter    started    INFO: bypass_norm_ratio = False
            2017-05-25 17:22:26,401    LoggerReporter    started    INFO: pair_identifier = R1_all
            2017-05-25 17:22:26,401    LoggerReporter    started    INFO: log_level = INFO
            2017-05-25 17:22:26,401    LoggerReporter    started    INFO: input_extension = fastq.gz
            2017-06-05 01:18:33,724    LoggerReporter    task_command    INFO: Executing with shell:  humann2 --input /data/path/sample1.fastq --output /data/path/humann2
            2017-06-05 05:08:55,051    LoggerReporter    task_command    INFO: Executing with shell:  humann2 --input /data/path/sample2.fastq --output /data/path/humann2
            2017-06-05 14:19:25,424    LoggerReporter    task_command    INFO: Executing with shell:  kneaddata --input /data/path/sample1.fastq --output /data/path/kneaddata
            2017-05-30 12:56:40,169    LoggerReporter    task_command    INFO: Tracked executable version:  kneaddata v0.6.0
            2017-05-30 18:58:28,797    LoggerReporter    task_command    INFO: Tracked executable version:  humann2 v0.11.0
            2017-05-31 04:09:29,594    LoggerReporter    task_command    INFO: Tracked executable version:  MetaPhlAn version 2.6.0    (19 August 2016)
            2017-05-25 17:24:54,329    root    run_task_command    INFO: Running commands for task id 1:
            humann2 --input /data/path/sample1.fastq --output /data/path/humann2
            2017-05-25 17:24:54,329    root    run_task_command    INFO: Running commands for task id 2:
            humann2 --input /data/path/sample2.fastq --output /data/path/humann2
            2017-05-25 17:36:14,569    root    record_benchmark    INFO: Benchmark information for job id 2:
            Elapsed Time: 00:04:31 
            Cores: 8
            Memory: 10 MB
            2017-05-25 17:36:14,569    root    record_benchmark    INFO: Benchmark information for job id 1:
            Elapsed Time: 00:05:31 
            Cores: 8
            Memory: 20 MB
            2017-05-25 17:36:54,317    LoggerReporter    log_event    INFO: task 3, kneaddata : ready and waiting for resources 
            2017-05-25 17:36:54,323    LoggerReporter    log_event    INFO: task 3, kneaddata : starting to run
        """
        # write the log to a temp file
        self.demo_log_file=os.path.join(self.workdir,"demo.log")
        with open(self.demo_log_file,"wb") as file_handle:
            file_handle.write(self.demo_log_text.encode("utf-8"))

    def tearDown(self):
        # remove temp files
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)

    def test_read_log_commands(self):
        # test reading the log to get the commands with out paths
        commands = LoggerReporter.read_log(self.demo_log_file, "commands")
        expected_commands=["humann2 --input sample1.fastq --output humann2",
            "humann2 --input sample2.fastq --output humann2",
            "kneaddata --input sample1.fastq --output kneaddata"]
        self.assertEqual(commands, expected_commands)
        
    def test_read_log_commands_paths(self):
        # test reading the log to get the commands with full paths
        commands = LoggerReporter.read_log(self.demo_log_file, "commands", remove_paths=False)
        expected_commands=["humann2 --input /data/path/sample1.fastq --output /data/path/humann2",
            "humann2 --input /data/path/sample2.fastq --output /data/path/humann2",
            "kneaddata --input /data/path/sample1.fastq --output /data/path/kneaddata"]
        self.assertEqual(commands, expected_commands)
        
    def test_read_log_versions(self):
        # test reading the log to get the software versions
        versions = LoggerReporter.read_log(self.demo_log_file, "versions")
        expected_versions=["kneaddata v0.6.0","humann2 v0.11.0","MetaPhlAn version 2.6.0    (19 August 2016)"]
        self.assertEqual(versions, expected_versions)
        
    def test_read_log_benchmark(self):
        # test reading the benchmarking information from the log
        benchmarking = LoggerReporter.read_log(self.demo_log_file, "benchmarking")
        expected_benchmarking={"1":"humann2\tsample1.fastq\t00:05:31\t8\t20 MB",
            "2":"humann2\tsample2.fastq\t00:04:31\t8\t10 MB"}
        self.assertEqual(benchmarking,expected_benchmarking)
        
