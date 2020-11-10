**AnADAMA2 User Manual**
========================

AnADAMA2 is the next generation of AnADAMA (Another Automated Data Analysis Management Application). AnADAMA is a tool to create reproducible workflows and execute them efficiently. AnADAMA operates in a make-like manner using targets and dependencies of each task to allow for parallelization. In cases where a workflow is modified or input files change, only those tasks impacted by the changes will be rerun. 
 
Tasks can be run locally or in a grid computing environment to increase efficiency. AnADAMA2 includes meta-schedulers for SLURM and SGE grids. Tasks that are specified to run on the grid will be submitted and monitored. If a task exceeds its time or memory allotment it will be resubmitted with double the time or memory (based on which resource needs to be increased) at most three times. Benchmarking information of time, memory, and cores will be recorded for each task run on the grid.
 
Essential information from all tasks is recorded, using the default logger and command line reporters, to ensure reproducibility. The information logged includes the command line options provided to the workflow, the function or command executed for each task, versions of tracked executables, and any output (stdout or stderr) from a task. Information reported on the command line includes status for each task (ie which task is ready, started, or running) along with an overall status of percent and total tasks complete for the workflow. A auto-doc feature allows for workflows to generate documentation automatically to further ensure reproducibility by capturing the latest essential workflow information.
 
AnADAMA2 was architected to be modular allowing users to customize the application by subclassing the base grid meta-schedulers, reporters, and tracked objects (ie files, executables, etc). 


-------

.. contents:: **Table of Contents**

-------


**Features**
............

* Captures your workflow steps along with the specific inputs, outputs, and environment used for each of your workflow runs
* Parallel workflow execution on your local machine or in a grid compute environment
* Ability to rerun a workflow, executing only sub-steps, based on changes in dependencies

-------

**Installation**
................

AnADAMA2 is easy to install.

**Requirements**
----------------

Python 2.7+ is required. All other basic dependencies will be installed when installing AnADAMA2.

The workflow documentation feature uses `Pweave <http://mpastell.com/pweave>`_ which will automatically be installed for you when installing AnADAMA2 and `Pandoc <http://pandoc.org/installing.html>`_. For workflows that use the documentation feature, `matplotlib <http://matplotlib.org/users/installing.html>`_ (version2+ required), `Pandoc <http://pandoc.org/installing.html>`_ (<version2 required), and `LaTeX <https://www.latex-project.org/get/>`_ will need to be installed manually. If your document includes hclust2 heatmaps, `hclust2 <https://bitbucket.org/nsegata/hclust2/overview>`_ will also need to be installed.

**Install**
------------------------

Run the following command to install AnADAMA2 and dependencies:
::

    $ pip install anadama2

Add the option ``--user`` to the install command if you do not have root permissions.

**Test**
--------

Once you have AnADAMA2 installed, you can optionally run the unit tests. To run the unit tests, change directories into the AnADAMA2 install folder and run the following command: 

:: 

    $ python setup.py test

-------

**Basic Usage**
...............

AnADAMA2 does not install an executable that you would run from the command line. Instead, you define your workflows as a Python script; ``anadama2`` is a module you import and use to describe your workflow.

**Definitions**
---------------

Before we get started with a basic workflow, there are a couple important definitions to review.

* Workflow

  - A collection of tasks.

* Task
  
  - A unit of work in the workflow.
  - A task has at least one action, zero or more targets, and zero or more dependencies.

* Target

  - An item that is created or modified by the task (ie like writing to a file).
  - All targets must exist after a task is run (they might not exist before the task is run).

* Dependency

  - An item that is required to run the task (ie input file or variable string).
  - All dependencies of a task must exist before the task can be run.

Targets and dependencies can be of different formats. See the section on "Types of Tracked Items" for all of the different types.

Tasks are run by executing all of its actions after all of its dependencies exist. After a task is run, it's marked as successful if no Python exceptions were raised, no shell commands had a non-zero exit status, and all of the targets were created.

**Run a Basic Workflow**
------------------------

A basic workflow script can be found in the examples folder in the source repository named ``exe_check.py``. The script ``exe_check.py`` gets a list of the global executables and also the local executables (those for the user running the script). It then checks to see if there are any global executables that are also installed locally. This script shows how to specify dependencies and targets in the commands directly. Lines 4-6 of the example script show targets with the format ``[t:file]`` and dependencies with the format ``[d:file]``. 

To run this example simply execute the script directly:

::

    $ python exe_check.py

The contents of this script are as follows (line numbers are shown for clarity):

::

    1 from anadama2 import Workflow
    2
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.do("ls /usr/bin/ | sort > [t:global_exe.txt]")
    5 workflow.do("ls $HOME/.local/bin/ | sort > [t:local_exe.txt]")
    6 workflow.do("join [d:global_exe.txt] [d:local_exe.txt] > [t:match_exe.txt]")
    7 workflow.go()

The first line imports AnADAMA2 and the third line creates an instance of the Workflow class removing the command line options input and output as they are not used for this workflow. These two lines are required for every AnADAMA2 workflow. Lines 4-6 add tasks to the workflow and line 7 tells AnADAMA2 to execute the tasks.

**Command Line Interface**
--------------------------

All AnADAMA2 workflows have a command line interface that includes a few default arguments. The default arguments include an input folder, an output folder, and the number of tasks to run in parallel. See the section "Run an Intermediate Workflow" for information on how to add custom options.

For a full list of options, run your workflow script with the "--help" option.

::

    $ python exe_check.py --help
    usage: exe_check.py [options]

    AnADAMA2 Workflow
    Options:
      --version             show program's version number and exit
      -h, --help            show this help message and exit
      -j JOBS, --local-jobs=JOBS
                            The number of tasks to execute in parallel locally.
      -t TARGET, --target=TARGET
                            Only execute tasks that make these targets. Use this
                            flag multiple times to build many targets. If the
                            provided value includes ? or * or [, treat it as a
                            pattern and build all targets that match.
      -d, --dry-run         Print tasks to be run but don't execute their actions.
      -l, --deploy          Create directories used by other options
      -T EXCLUDE_TARGET, --exclude-target=EXCLUDE_TARGET
                            Don't execute tasks that make these targets. Use this
                            flag multiple times to exclude many targets. If the
                            provided value includes ? or * or [, treat it as a
                            pattern and exclude all targets that match.
      -u UNTIL_TASK, --until-task=UNTIL_TASK
                            Stop after running the named task. Can refer to the
                            end task by task number or task name.
      -e, --quit-early      If any tasks fail, stop all execution immediately. If
                            not set, children of failed tasks are not executed but
                            children of successful or skipped tasks are executed.
                            The default is to keep running until all tasks that
                            are available to execute have completed or failed.
      -g GRID, --grid=GRID  Run gridable tasks on this grid type.
      -U EXCLUDE_TASK, --exclude-task=EXCLUDE_TASK
                            Don't execute these tasks. Use this flag multiple
                            times to not execute multiple tasks.
      -i INPUT, --input=INPUT
                            Collect inputs from this directory.
      -o OUTPUT, --output=OUTPUT
                            Write output to this directory. By default the
                            dependency database and log are written to this
                            directory
      -n, --skip-nothing    Skip no tasks, even if you could; run it all.
      -J GRID_JOBS, --grid-jobs=GRID_JOBS
                            The number of tasks to submit to the grid in parallel.
                            The default setting is zero jobs will be run on the
                            grid. By default, all jobs, including gridable jobs,
                            will run locally.
      -p GRID_PARTITION, --grid-partition=GRID_PARTITION
                            Run gridable tasks on this partition.
      --config              Find workflow configuration in this folder
                            [default: only use command line options] 

Options can be provided on the command line or included in a config file with the option "--config=FILE". The config file should be of the format
similar to Microsoft Windows INI files with a section plus key value pairs. No specific section name is required but "default" is recommended.
An example config file is included below setting two custom options. Please note the settings in the config file override the default
option settings while command line options override the config file settings. More specifically if a user were to set an option in the 
config file and also on the command line the value on the command line is the one that will be used for the workflow. All options set,
those that are defaults, included on the command line or in the config file, are written to the log each time the workflow is run to
capture the exact run-time settings.

::

    [default]
    input_extension = fastq.gz
    output_extension = bam


**Console Output**
------------------

As tasks are run progress information is printed to the console. The default reporter prints at least five types of information to standard output each time a status message for a task is recorded:

1. The local date/time for the message is printed first.
2. The progress of the workflow is represented as three numbers. The first number is the number of tasks that finished running. The second number is the total number of tasks in the workflow. The last number is the percent completion for the workflow. 
3. The status of the task is printed. Tasks can be skipped or started and they could complete or fail. There are a total of six different status messages.

   a. ``Ready``: All dependencies for the task are available. The task is in the queue waiting for computational resources. For example, if there are 10 tasks that are ready and 10 jobs were specified to run at one time, all ready tasks will immediately start running. If there are more ready tasks then jobs specified, these tasks will wait until other jobs have finished running before starting.
   b. ``Started``: The task started running locally or is about to submit a job to the grid depending on if a task is gridable and if the command line options specified a grid to be used.
   c. ``Completed``: The task finished running without error.
   d. ``Failed``: The task stopped running and an error was reported.
   e. ``Skipped``: The task has been skipped. It does not need to be run because the targets of the task exist and have newer timestamps than the dependencies.
   f. ``GridJob``: The task has been submitted to the grid. This status indicates incremental status messages are included at the end of the message about the status of the grid job.

4. The task number is included to identify the task associated with the status message.
5. A description for the task is included which by default is the task name. If a task does not have a name the description is the first task action. If this action is a command the description is the executable name. If this action is a function the description is the function name. The description is limited to keep the status line to at most 79 characters. The total number of characters required are based on the total number of tasks, since more tasks require more padding for the formatting of the progress section and task number parts of this message to keep all sections in their respective columns. If a description is truncated, it will be followed with an ellipsis (ie "Very Long Task Description is Reduced ...").
6. If a grid is selected, additional status information is printed including job submission and states. This additional column in some cases can increase the total line length to more than 79 characters.

Here is an example of the output from running a workflow of kneaddata and humann2 tasks on two input files with two local jobs running in parallel::

  (Dec 08 11:50:43) [0/4 -   0.00%] **Ready    ** Task 2: kneaddata
  (Dec 08 11:50:43) [0/4 -   0.00%] **Started  ** Task 2: kneaddata
  (Dec 08 11:50:43) [0/4 -   0.00%] **Ready    ** Task 0: kneaddata
  (Dec 08 11:50:43) [0/4 -   0.00%] **Started  ** Task 0: kneaddata
  (Dec 08 11:50:44) [1/4 -  25.00%] **Completed** Task 2: kneaddata
  (Dec 08 11:50:44) [1/4 -  25.00%] **Ready    ** Task 5: humann2
  (Dec 08 11:50:44) [1/4 -  25.00%] **Started  ** Task 5: humann2
  (Dec 08 11:50:44) [2/4 -  50.00%] **Completed** Task 0: kneaddata
  (Dec 08 11:50:44) [2/4 -  50.00%] **Ready    ** Task 4: humann2
  (Dec 08 11:50:44) [2/4 -  50.00%] **Started  ** Task 4: humann2
  (Dec 08 11:52:48) [3/4 -  75.00%] **Completed** Task 5: humann2
  (Dec 08 11:52:49) [4/4 - 100.00%] **Completed** Task 4: humann2
  Run Finished


Here is an example of the output from running a kneaddata workflow on three input files using a grid and allowing two grid jobs at a time::

  (Dec 08 14:33:07) [0/3 -   0.00%] **Ready    ** Task 4: kneaddata
  (Dec 08 14:33:07) [0/3 -   0.00%] **Started  ** Task 4: kneaddata
  (Dec 08 14:33:07) [0/3 -   0.00%] **Ready    ** Task 2: kneaddata
  (Dec 08 14:33:07) [0/3 -   0.00%] **Started  ** Task 2: kneaddata
  (Dec 08 14:33:07) [0/3 -   0.00%] **Ready    ** Task 0: kneaddata
  (Dec 08 14:33:25) [0/3 -   0.00%] **GridJob  ** Task 4: kneaddata <Grid JobId 76918649 : Submitted>
  (Dec 08 14:33:34) [0/3 -   0.00%] **GridJob  ** Task 2: kneaddata <Grid JobId 76918676 : Submitted>
  (Dec 08 14:34:25) [0/3 -   0.00%] **GridJob  ** Task 4: kneaddata <Grid JobId 76918649 : PENDING>
  (Dec 08 14:47:26) [0/3 -   0.00%] **GridJob  ** Task 4: kneaddata <Grid JobId 76918649 : PENDING>
  (Dec 08 14:47:26) [0/3 -   0.00%] **GridJob  ** Task 4: kneaddata <Grid JobId 76918649 : Getting benchmarking data>
  (Dec 08 14:47:35) [0/3 -   0.00%] **GridJob  ** Task 2: kneaddata <Grid JobId 76918676 : PENDING>
  (Dec 08 14:47:35) [0/3 -   0.00%] **GridJob  ** Task 2: kneaddata <Grid JobId 76918676 : Getting benchmarking data>
  (Dec 08 14:49:35) [0/3 -   0.00%] **GridJob  ** Task 4: kneaddata <Grid JobId 76918649 : Final status of COMPLETED>
  (Dec 08 14:49:35) [0/3 -   0.00%] **GridJob  ** Task 2: kneaddata <Grid JobId 76918676 : Final status of COMPLETED>
  (Dec 08 14:49:35) [0/3 -   0.00%] **Started  ** Task 0: kneaddata
  (Dec 08 14:49:35) [1/3 -  33.33%] **Completed** Task 4: kneaddata
  (Dec 08 14:49:35) [2/3 -  66.67%] **Completed** Task 2: kneaddata
  (Dec 08 14:49:44) [2/3 -  66.67%] **GridJob  ** Task 0: kneaddata <Grid JobId 76922265 : Submitted>
  (Dec 08 14:50:44) [2/3 -  66.67%] **GridJob  ** Task 0: kneaddata <Grid JobId 76922265 : Waiting>
  (Dec 08 14:50:44) [2/3 -  66.67%] **GridJob  ** Task 0: kneaddata <Grid JobId 76922265 : Getting benchmarking data>
  (Dec 08 14:54:46) [2/3 -  66.67%] **GridJob  ** Task 0: kneaddata <Grid JobId 76922265 : Final status of COMPLETED>
  (Dec 08 14:54:46) [3/3 - 100.00%] **Completed** Task 0: kneaddata
  Run Finished


**Logging Output**
------------------

A default setting for workflows is to thoroughly log execution information to a text file named ``anadama.log`` in the current directory.  This log contains information how tasks relate to one another, why tasks were skipped or not skipped, action information like the exact shell command used (if a task dispatched a command to the shell), and full traceback information for all exceptions. With each workflow executed the log appends. It will include information on all runs for a workflow. Below is an example of what to expect from the ``anadama.log``::

  2016-10-28 09:07:38,613	anadama2.runners	_run_task_locally	DEBUG: Completed executing task 3 action 0
  2016-10-28 09:07:38,613	LoggerReporter	task_completed	INFO: task 3, `Task(name="sed 's|.*Address: \\(.*[0-9]\\)<.*|\\1|' my_ip.txt > ip.txt", actions=[<function actually_sh at 0x7f32ff082410>], depends=[<anadama2.tracked.TrackedFile object at 0x7f32ff07dfd0>, <anadama2.tracked.TrackedVariable object at 0x7f32ff07df90>, <anadama2.tracked.TrackedExecutable object at 0x7f32ff092050>], targets=[<anadama2.tracked.TrackedFile object at 0x7f32ff092190>], task_no=3)' completed successfully.
  2016-10-28 09:07:38,613	LoggerReporter	task_started	INFO: task 5, `whois $(cat ip.txt) > whois.txt' started. 2 parents: [3, 4].  0 children: [].
  2016-10-28 09:07:38,613	anadama2.runners	_run_task_locally	DEBUG: Executing task 5 action 0
  2016-10-28 09:07:38,613	anadama2.helpers	actually_sh	INFO: Executing with shell: whois $(cat ip.txt) > whois.txt
  2016-10-28 09:07:38,875	anadama2.helpers	actually_sh	INFO: Execution complete. Stdout: 
  Stderr: 
  2016-10-28 09:07:38,875	anadama2.runners	_run_task_locally	DEBUG: Completed executing task 5 action 0
  2016-10-28 09:07:38,875	LoggerReporter	task_completed	INFO: task 5, `Task(name='whois $(cat ip.txt) > whois.txt', actions=[<function actually_sh at 0x7f32ff082488>], depends=[<anadama2.tracked.TrackedFile object at 0x7f32ff092190>, <anadama2.tracked.TrackedVariable object at 0x7f32ff07df10>, <anadama2.tracked.TrackedExecutable object at 0x7f32ff0921d0>], targets=[<anadama2.tracked.TrackedFile object at 0x7f32ff092390>], task_no=5)' completed successfully.
  2016-10-28 09:07:38,875	LoggerReporter	finished	INFO: AnADAMA run finished.



**Run with Specific Targets**
-----------------------------

Tasks in each workflow can have one or more targets. A target is an item, usually a file, that is created or modified by a task. With AnADAMA2, you can select specific targets for each of your runs. This is useful in that it allows you to only run a portion of your workflow.

Targets for each run are selected through the command line interface. The options that allow you to specify a target or set of targets can be shown by running your workflow with the help option. See the prior section for the full list.

Running the example workflow with the target option would only create the "global_exe.txt" file. Targets that do not include a full path are expected to be located relative to your current working directory. If a target is in a different folder, provide the full path to the target.

::

    $ python check_exe.py --target global_exe.txt

Running the example workflow with the expanded target option would only create the "global_exe.txt" and the "local_exe.txt" files. For targets with patterns, place the target in quotes so the pattern is not evaluated by the shell.

::

    $ python check_exe.py --target "*l_exe.txt"


**Output Files**
----------------

AnADAMA2 will always place at least two items in the output folder. The output folder by default is the directory of your workflow script. If in your workflow, you remove the output folder the database will be written to your home directory and the log will be written to your current working directory. All workflows without output folders will share the same database. Currently it is not possible to run two workflows at once that share the same database.

**Database**
~~~~~~~~~~~~

AnADAMA2 stores a target and dependencies tracking database in the output folder for each run. This includes information on all of the items AnADAMA2 tracks from the workflow. You shouldn't need to look at this file, but you can remove it to erase all history of past runs.

**Log**
~~~~~~~

The log file will contain information on all of the tasks run including standard out and standard error from every command line task. It will also include the values of all workflow arguments.


**Other Basic Workflow Examples**
---------------------------------

Example AnADAMA2 workflow scripts can be found in the examples folder of the source download. Three of these example scripts are described below.

The script ``simple.py`` downloads a file and then runs two linux commands sed and whois. This script shows how to specify dependencies and targets in the commands directly. 

To run this example simply execute the script directly: 

::

    $ python simple.py

The script ``has_a_function.py`` shows how to add a task which is a function to a workflow. This script will download a file, decompress the file, and then remove the trailing tabs in the file. 

To run this example simply execute the script directly:

::

    $ python has_a_function.py

The script ``kvcontainer.py`` shows how to use a container as a dependency. This container allows you to track a set of strings without having them overlap with other strings of the same name in another workflow. 

To run this example simply execute the script directly:

::

    $ python kvcontainer.py


**Jupyter Notebook Examples**
---------------------------------

Examples of running AnADAMA2 in Jupyter Notebooks are included in the examples folder. `Jupyter nbviewer <http://nbviewer.jupyter.org/>`_ can be used to render the notebooks online.

The first notebook shows a simple example on how to download files. To see the notebook rendered with nbviewer visit `AnADAMA2_download_files_example.ipynb <http://nbviewer.jupyter.org/urls/bitbucket.org/biobakery/anadama2/raw/tip/examples/jupyter_notebooks/AnADAMA2_download_files_example.ipynb>`_.

The second notebook illustrates a more complex example of processing shotgun sequencing data. This example requires additional dependencies be installed prior to running. It also requires input files and database files to be downloaded. To see the notebook rendered with nbviewer visit `AnADAMA2_shotgun_workflow_example.ipynb <http://nbviewer.jupyter.org/urls/bitbucket.org/biobakery/anadama2/raw/tip/examples/jupyter_notebooks/AnADAMA2_shotgun_workflow_example.ipynb>`_. 

-------

**Intermediate Usage**
......................


**Add a Task**
---------------------

The do function allows you to specify targets and dependencies in the task command with the special formatting surrounding the full paths to specific files. These tasks have binaries and also the full commands tracked. The do function can be replaced with the add_task function to provide you with additional types of targets and dependencies. Taking the example workflow and replacing lines 4-6 with add_task functions instead of do functions you can see how the syntax changes.

:: 

    1 from anadama2 import Workflow
    2
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.add_task("ls /usr/bin/ | sort > [targets[0]]", targets="global_exe.txt")
    5 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    6 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets="match_exe.txt")
    7 workflow.go()

**Sections of a Workflow**
----------------------------------

An intermediate workflow can contain five sections. Some of these sections are optional to include based on your workflow. The example workflow below has one task which is to run MetaPhlAn2 on a set of fastq input files.  

:: 

    1 ### Section #1: Import anadama2 and create a workflow instance (Required)
    2 from anadama2 import Workflow
    3 workflow = Workflow(version="0.0.1", description="A workflow to run MetaPhlAn2" )
    4
    5 ### Section #2: Add custom arguments and parse arguments (Optional)
    6 workflow.add_argument("input-extension", desc="the extensions of the input files", default="fastq")
    7 args = workflow.parse_args()
    8
    9 ### Section #3: Get input/output file names (Optional)
    10 in_files = workflow.get_input_files(extension=args.input_extension)
    11 out_files = workflow.name_output_files(name=in_files, tag="metaphlan2_taxonomy")
    12
    13 ### Section #4: Add tasks (Required)
    14 workflow.add_task_group("metaphlan2.py [depends[0]] --input_type [extension] > [targets[0]]", depends=in_files, targets=out_files, extension=args.input_extension)
    15
    16 ### Section #5: Run tasks (Required)   
    17 workflow.go()


**Section 1** is required. In this section the anadama2 workflow is imported and a workflow instance is created. This section needs to be included in all AnADAMA2 workflows basic, intermediate, or advanced. Line 3 adds the version and description information to the workflow. This is optional. These values will be printed when using the command line ``--version`` and ``--help`` options. 

**Section 2** is optional. In this section custom arguments are added to the command line options. Next the workflow parses the options. This command is not required as by default the workflow will parse the arguments from the command line when they are needed by the workflow. Because the options are parsed on demand, it is not possible to add custom arguments later on in the workflow, like after running tasks, because the command line arguments are needed before tasks can be created and run.

**Section 3** is optional. These two helper functions allow you to easily get input and output files. You do not need to include section 2 in you workflow to use these functions. The first function will return all of the input files with the extension provided in the input folder. The input folder can be provided by the user on the command line. The default location of the input folder is the current working directory. The output files function will only return the name of the output files providing the basenames, extensions, and tags. These two functions differ in that the input files function will search the input folder for files with the selected properties (ie. extension) while the output files function will only return full path names. The output files function will not search the output folder as the files to be named likely do not exist yet and will be created by the tasks. The output folder is a required option which can be set in the workflow or can be provided on the command line by the user.

**Section 4** is required. In all workflows, tasks will be added. These tasks can be added with ``do``, ``add_task``, ``add_task_group``, ``do_gridable``, ``add_task_gridable``, and ``add_task_group_gridable`` functions. The gridable functions allow for tasks to be run on a grid. See the grid computing section for more information. Tasks can have targets and dependencies. Targets are usually files that are created by the task and dependencies are usually files that are required as input for the task. Optionally parameters can be provided to the ``add_task`` function with their names substituted into the command if written in the square bracket format. For example, the task in the example workflow replaces ``[extension]`` in the command with the variable provided so it evaluated as ``args.input_extension`` which is the value of the input file extension, either the default or the value provided by the user on the command line.

**Section 5** is required. In all workflows, there must be at least one call to the ``go`` function. This is the function that will start the tasks running. If this function is not included, none of the tasks will be run.


**Run an Intermediate Workflow**
---------------------------------

The intermediate workflow shown below will run a set of fastq files with KneadData. This script is named ``kneaddata_workflow.py`` and can be found in the examples folder. Line 3 adds version information and a description to the workflow. Line 5 adds a custom option so that the user can provide the location of the kneaddata database on the command line with ``--kneaddata_db <folder>``. Line 6 adds another custom option allowing the user to set the input file extensions. Line 8 parses the arguments from the command line and returns their values. Line 10 returns a list of all of the files in the input folder with the extension provided. Line 11 returns the matched output files replacing the input folder location with the output folder and the input extension with that expected from kneaddata. Line 13 adds a group of tasks. One task is added for each of the input and matched output files. Line 15 executes the tasks.

:: 

    1 from anadama2 import Workflow
    2
    3 workflow = Workflow(version="0.0.1", description="A workflow to run KneadData" )
    4
    5 workflow.add_argument(name="kneaddata_db", description="the kneaddata database", default="/work/code/kneaddata/db/")
    6 workflow.add_argument(name="input_extension", description="the input file extension", default="fastq")
    7 workflow.add_argument("threads", desc="number of threads for kneaddata to use", default=1)
    8 args = workflow.parse_args()
    9
    10 in_files = workflow.get_input_files(extension=args.input_extension)
    11 out_files = workflow.name_output_files(name=in_files, tag="kneaddata")
    12
    13 workflow.add_task_group("kneaddata -i [depends[0]] -o [output_folder] -db [kneaddata_db] -t [threads]", depends=in_files, targets=out_files, output_folder=args.output, kneaddata_db=args.kneaddata_db, threads=args.threads)
    14   
    15 workflow.go()

To see the version number for this workflow run:

:: 

    $ python kneaddata_workflow.py --version

To print the options (default and custom) along with the custom description for this workflow run:

:: 

    $ python kneaddata_workflow.py --help

To run the workflow (providing the input folder, output folder, extension, and threads):

:: 

    $ python kneaddata_workflow.py --input input_dir --output output_dir --input_extension .fastq.gz --threads 4

**Add a Document**
---------------------

A document can be added to the workflow to allow for automatic generation of reports including text, tables, plots, and heatmaps based on data generated from the workflow tasks. The reports can be formatted as any `Pandoc <http://pandoc.org/installing.html>`_ output type (ie pdf, html, html5, json, docx, etc). The report format requested is indicated by the extension of the report file name.

The default document feature uses `Pweave <http://mpastell.com/pweave>`_ and `Pandoc <http://pandoc.org/installing.html>`_. Alternatively, a custom documentation class can be provided to your workflow instance if you would like to customize the documentation feature.

**Document Template**

To add a document to your workflow, first create a Pweave template. It can be formatted as any Pweave input type (ie pandoc markdown, script, tex, or rst). For more information on these formats, see the `Pweave documentation <http://mpastell.com/pweave>`_. The template file extension indicates the file format with ``.py`` for python and ``.mdw`` for Pandoc markdown. 

The templates can use the AnADAMA2 document to add features like tables, plots, and heatmaps. For more examples\, see the `bioBakery Workflows <https://bitbucket.org/biobakery/biobakery_workflows/wiki/Home>`_ document templates.

*Example Template*

:: 

    1 #+ echo=False
    2 import time
    3 from anadama2 import PweaveDocument
    4 document = PweaveDocument()
    5 
    6 vars = document.get_vars()
    7 
    8 #' % <% print(vars["title"]) %>
    9 #' % Project: <% print(vars["project"]) %>
    10 #' % Date: <%= time.strftime("%m/%d/%Y") %>
    11
    12 #' # Introduction
    13 #' <% print(vars["introduction_text"]) %>
    14
    15 #' # Functional profiling
    16
    17 #' This report section contains information about the functional profiling run
    18 #' on all samples. These samples were
    19 #' run through [HUMAnN2](http://huttenhower.sph.harvard.edu/humann2).
    20 
    21 #+ echo=False
    22 samples, pathways, data = document.read_table(vars["file_pathabundance_unstratified"])
    23 
    24 #' There were a total of <% print(len(samples)) %> samples run for this data set. Overall
    25 #' there were <% print(len(pathways)) %> pathways identified.


* Line 1: Start a python section indicating that the code should not be written to the document.

  - If echo is set to true, the text will appear in the document. 

* Lines 3 and 4: Import the default AnADAMA2 document and create an instance. 
* Line 6: Get the variables for the document.

  - The variables are the "vars" argument provided to the "add_document" function in the workflow.
  - The variables for this template are "title", "project", "introduction_text", and "file_pathabundance_unstratified". 

* Line 8: Add a title using the document variable "title".
* Line 9: Add the project name to the report.
* Line 10 Add the current date. 
* Line 12: Add the introduction section header.
* Line 13: Add the introduction text.
* Line 19: Add text including a link to the HUMAnN2 landing page.
* Line 21: Start a section of python which will not be echoed to the document.
* Line 22: Read the pathway abundance file returning sample names, pathway names, and the data matrix.
* Lines 24-25: Compute the total number of samples and pathways and add them to the generated text.

**Workflow to Create Document**

Next create a workflow that includes a document. A workflow that creates a document can be short with just code to create a workflow instance, add a document, and then run go to start the workflow. This example workflow will run all of the fastq.gz files in the input folder through humann2, then merge the pathway abundance output files, and split the file to create a stratified and unstratified pathway abundance file with data for all samples.

*Example Workflow*

:: 

    1 from anadama2 import Workflow
    2
    3 workflow = Workflow(version="0.1", description="A workflow to run hummann2 and create a document")
    4 
    5 args = workflow.parse_args()
    6
    7 input_files = workflow.get_input_files(extension=".fastq.gz")
    8 humann2_output_files = workflow.name_output_files(name=input_files, tag="pathabundance", extension="tsv")
    9 humann2_merged_pathabundance = workflow.name_output_files("merged_pathabundance.tsv")
    10 humann2_merged_pathab_unstrat = workflow.name_output_files("merged_pathabundance_unstratified.tsv")
    11 document_file = workflow.name_output_files("functional_profiling_document.pdf")
    12
    13 workflow.add_task_group(
    14    "humann2 --input [depends[0]] --output [vars[0]]",
    15    depends=input_files,
    16    targets=humann2_output_files,
    17    vars=args.output)
    18
    19 workflow.add_task(
    20    "humann2_join_tables --input [vars[0]] --output [targets[0]] --file_name pathab",
    21     depends=humann2_output_files,
    22     targets=humann2_merged_pathabundance,
    23     vars=args.output)
    24
    25 workflow.add_task(
    26    "humann2_split_stratified_table --input [depends[0]] --output [vars[0]]",
    27     depends=humann2_merged_pathabundance,
    28     targets=humann2_merged_pathab_unstrat,
    29     vars=args.output)
    30
    31 workflow.add_document(
    32    templates="template.py",
    33    depends=humann2_merged_pathab_unstrat,
    34    targets=document_file,
    35    vars={"title":"Demo Title",
    36        "project":"Demo Project",
    37        "introduction_text":"This is a demo document.",
    38        "file_pathabundance_unstratified":humann2_merged_pathab_unstrat})
    39
    40 workflow.go()

* Lines 13-17: Add a group of tasks to run HUMAnN2 on each fastq.gz input file.
* Lines 19-23: Add a task to join the pathway abundance tables for each sample.
* Lines 25-26: Add a task to split the pathway abundance table into a stratified and unstratified table. 
* Lines 31-38: Add a document to the workflow.

  - In this example the template file is named "template.py". It is a python script with pandoc markdown.
  - Depends and targets are set for documents just like for tasks since generating a document is a task. 
  - The variables provided to the document task in lines 35-38 are passed to the document template. 
  - Document variables can be any python object that is pickleable. 
  - The document file generated is named "functional_profiling_document.pdf" and is located in the output folder. Changing the document file name to "functional_profiling_document.html" would generate a html report.
  - To add a table of contents to your report, add ``table_of_contents=True`` to the add_document function.

**Run the Workflow**

Finally run the workflow with python. For this example, if the workflow is saved as a file named "example_doc_workflow.py", it would be run as follows.

:: 

    $ python example_doc_workflow.py --input input_folder --output output_folder

The document created will look like the following image (with the date replaced with the date the workflow was run).

.. image:: https://bitbucket.org/repo/gaqz78/images/3478728655-Screenshot%20from%202017-01-18%2017-04-22.png


**Add an Archive**
-------------------

An archive can be added to a workflow to allow for automated generation of a packaged file containing products from various steps in the workflow. The user specifies which items are included in the archive and the type of archive. Archives are useful for packaging documents plus their corresponding figures and data files.

To add an archive to your workflow, call the ``add_archive`` workflow function with the path to the archive you would like to create along with the folders/files you would like to archive. Here is the document workflow example from the prior section with an archive added.

*Example Workflow*

:: 

    1 from anadama2 import Workflow
    2
    3 workflow = Workflow(version="0.1", description="A workflow to run hummann2 and create a document")
    4 
    5 args = workflow.parse_args()
    6
    7 input_files = workflow.get_input_files(extension=".fastq.gz")
    8 humann2_output_files = workflow.name_output_files(name=input_files, tag="pathabundance", extension="tsv")
    9 humann2_merged_pathabundance = workflow.name_output_files("merged_pathabundance.tsv")
    10 humann2_merged_pathab_unstrat = workflow.name_output_files("merged_pathabundance_unstratified.tsv")
    11 document_file = workflow.name_output_files("functional_profiling_document.pdf")
    12
    13 workflow.add_task_group(
    14    "humann2 --input [depends[0]] --output [vars[0]]",
    15    depends=input_files,
    16    targets=humann2_output_files,
    17    vars=args.output)
    18
    19 workflow.add_task(
    20    "humann2_join_tables --input [vars[0]] --output [targets[0]] --file_name pathab",
    21     depends=humann2_output_files,
    22     targets=humann2_merged_pathabundance,
    23     vars=args.output)
    24
    25 workflow.add_task(
    26    "humann2_split_stratified_table --input [depends[0]] --output [vars[0]]",
    27     depends=humann2_merged_pathabundance,
    28     targets=humann2_merged_pathab_unstrat,
    29     vars=args.output)
    30
    31 doc_task=workflow.add_document(
    32    templates="template.py",
    33    depends=humann2_merged_pathab_unstrat,
    34    targets=document_file,
    35    vars={"title":"Demo Title",
    36        "project":"Demo Project",
    37        "introduction_text":"This is a demo document.",
    38        "file_pathabundance_unstratified":humann2_merged_pathab_unstrat})
    39
    40 workflow.add_archive(
    41     targets=args.output+".zip",
    42     depends=[args.output,doc_task],
    43     remove_log=True)
    44
    45 workflow.go()


* Line 31: This line is modified to capture the add document task in a variable.
* Lines 40-43: These are the new lines added to add the archive. The target is the path to the archive that will be created. It is named the same as the output folder provided by the user on the command line. The list of dependencies are those items to include in the archive. This example includes the full output folder in the archive. Dependencies can also be tasks which must run before the archive is to be created. These task dependencies will determine when the archive will be rerun. For example whenever a new document is created the archive task will be rerun. The "zip" target extension sets the archive type. An optional parameter is set to indicate the workflow log should not be included in the archive.


**Run in a Grid Computing Environment**
---------------------------------------

Writing a workflow to run in a grid computing environment is very
similar to writing a workflow to run on your own local machine. We can
make just a few changes to modify the example "exe_check.py" workflow
to run all tasks on the grid. We just need to specify which tasks
we would like to run on the grid replacing ``do`` with ``do_gridable``.
These tasks by default will run locally. If the command line option ``--grid-run``
is provided the ``gridable`` tasks will be run on the grid partition provided.
In lines 4-6 we also add three arguments to specify
memory (in MB), the number of cores, and the time in minutes that each
task should be allowed in the queue.

:: 

    1 from anadama2 import Workflow
    2 
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.do_gridable("ls /usr/bin/ | sort > [t:global_exe.txt]", mem=20, cores=1, time=2)
    5 workflow.do_gridable("ls $HOME/.local/bin/ | sort > [t:local_exe.txt]", mem=20, cores=1, time=2)
    6 workflow.do_gridable("join [d:global_exe.txt] [d:local_exe.txt] > [t:match_exe.txt]", mem=20, cores=1, time=2)
    7 workflow.go()

To run this workflow with 2 tasks running in parallel on a slurm grid on partition general, run:

:: 

    $ python exe_check.py --grid-jobs 2

Another option is to submit only some of the tasks to the grid computing environment. This can be done by changing the ``do_gridable`` to ``do``. We can modify our example to only run the global executable check on the grid. 

:: 

    1 from anadama2 import Workflow
    2 
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.do_gridable("ls /usr/bin/ | sort > [t:global_exe.txt]", mem=20, cores=1, time=2)
    5 workflow.do("ls $HOME/.local/bin/ | sort > [t:local_exe.txt]")
    6 workflow.do("join [d:global_exe.txt] [d:local_exe.txt] > [t:match_exe.txt]")
    7 workflow.go()

Similarly if we were using ``add_task`` instead of ``do``, ``add_task_gridable`` will run the task on the grid while ``add_task`` will always run the task locally.

:: 

    1 from anadama2 import Workflow
    2 
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.add_task_gridable("ls /usr/bin/ | sort > [targets[0]]", targets="global_exe.txt", mem=20, cores=1, time=2)
    5 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    6 workflow.add_task("join [depends[0]] [depends[0]] > [targets[0]]", targets="match_exe.txt", depends=["global_exe.txt","local_exe.txt"])
    7 workflow.go()

If you are running with sun grid engine instead of slurm, you do not need to modify your workflow. The software will identify the grid engine installed and run on the grid available. Alternatively, when running your workflow provide the options ``--grid <slurm/sge>`` and ``--grid-partition <general>`` on the command line.

The time and memory requests for each task can be an integer or an equation. Time is always specified in minutes and memory is specified in MB. Equations can include the same formatting as task commands with replacement for dependency and core variables. For example ``time="10 / [cores]"`` would request times based on the number of cores for that specific task (ie. 10 minutes for 1 core and 5 minutes for 2 cores). Equations can also use the size of the files that the task depends on. For example, ``time="10 * [cores] * file_size('[depends[0]]')"`` could be used to set the time for a task. The ``file_size`` function is an AnADAMA2 helper function that returns the size of a file in GB. This equation will be evaluated for the time right before the task is to be started. This allows for equations to use the size of files that might not exist when the workflow starts but will exist before a task is set to start running.

-----------

**Advanced Usage**
..................

**Functions**
-------------

All of the tasks in the example are currently commands. They are strings that would be run on the command line. Instead of passing commands as a task you can provide functions. Here is the example after replacing line 5 with a function that writes a file with the list of the global executables.

:: 

    1 from anadama2 import Workflow
    2 from anadama2.util import get_name
    3
    4 workflow = Workflow(remove_options=["input","output"])
    5 def get_global_exe(task):
    6    import subprocess
    7    outfile=open(get_name(task.targets[0]), "w")
    8    process=subprocess.check_call("ls /usr/bin/ | sort", shell=True, stdout=outfile)
    9    outfile.close()
    10
    11 workflow.add_task(get_global_exe, targets="global_exe.txt")
    12 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    13 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets="match_exe.txt")
    14 workflow.go()

**Decorators**
--------------

A decorator is a function that takes another function as input and extends the behavior of the input function. Instead of providing a function as input to a task you can use the add_task to decorate your input function. Taking the example workflow which already included a function in the prior section and using the decorator method moves line 10 to line 4, right before the function get_global_exe.

:: 

    1 from anadama2 import Workflow
    2
    3 workflow = Workflow(remove_options=["input","output"])
    4 @workflow.add_task(targets="global_exe.txt")
    5 def get_global_exe(task):
    6    import subprocess
    7    outfile=open(task.targets[0].name, "w")
    8    process=subprocess.check_call("ls /usr/bin/ | sort", shell=True, stdout=outfile)
    9    outfile.close()
    10
    11 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    12 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets="match_exe.txt")
    13 workflow.go()

**Types of Tracked Items**
--------------------------

Targets and dependencies, both of which are tracked items, can be one of six types. They are provided to the function add_task.

**Files**
~~~~~~~~~

By default all targets and dependencies provided as strings to add_task are file dependencies. Depending on the size of the file these could be large file dependencies for which the checksums are not tracked to save time. We can specify the types of dependences as file and huge files in the add task function. In line 4 we specify a file dependency and in line 6 we specify a huge file dependency. In line 2 we import the AnADAMA2 dependency functions.

:: 

    1 from anadama2 import Workflow
    2 from anadama2.tracked import TrackedFile, HugeTrackedFile
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.add_task("ls /usr/bin/ | sort > [targets[0]]", targets=TrackedFile("global_exe.txt"))
    5 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    6 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets=HugeTrackedFile("match_exe.txt"))
    7 workflow.go()

**Directories**
~~~~~~~~~~~~~~~

Directories can be specified in the same way as file dependencies. In line 4 of the example, a directory dependency is added. In line 2 we import the AnADAMA2 dependency function.

:: 

    1 from anadama2 import Workflow
    2 from anadama2.tracked import TrackedDirectory
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.add_task("ls /usr/bin/ | sort > [targets[0]]", targets="global_exe.txt", depends=TrackedDirectory("/usr/bin/"))
    5 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    6 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets="match_exe.txt")
    7 workflow.go()

**Executables**
~~~~~~~~~~~~~~~

Executables can be specified in the same way as file and directory dependencies. Adding line 2 and then including an executable dependency in line 4 of the example adds the "ls" dependency.

:: 

    1 from anadama2 import Workflow
    2 from anadama2.tracked import TrackedExecutable
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.add_task("ls /usr/bin/ | sort > [targets[0]]", targets="global_exe.txt", depends=TrackedExecutable("ls"))
    5 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    6 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets="match_exe.txt")
    7 workflow.go()

**Functions**
~~~~~~~~~~~~~

Function dependencies are specified similar to the other dependencies. In the example workflow, modified to include the function, we add the function itself as a dependency.

:: 

    1 from anadama2 import Workflow
    2 from anadama2.tracked import TrackedFunction
    3 workflow = Workflow(remove_options=["input","output"])
    4 def get_global_exe(task):
    5    import subprocess
    6    outfile=open(task.targets[0].name, "w")
    7    process=subprocess.check_call("ls /usr/bin/ | sort", shell=True, stdout=outfile)
    8    outfile.close()
    9
    10 workflow.add_task(get_global_exe, targets="global_exe.txt", TrackedFunction(get_global_exe))
    11 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    12 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets="match_exe.txt")
    13 workflow.go()

**Containers**
~~~~~~~~~~~~~~

You can also have tasks that depend on variables or arguments to your task actions. In our example, say we only want the first N global executables. We would create a container with our variable name and then provide it as one of the task dependencies.

:: 

    1 from anadama2 import Workflow
    2 from anadama2.tracked import Container
    3 workflow = Workflow(remove_options=["input","output"])
    4 variables=Container(lines=2)
    5 workflow.add_task("ls /usr/bin/ | sort | head -n [depends[0]] > [targets[0]]", targets="global_exe.txt", depends=variables.lines)
    6 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    7 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets="match_exe.txt)
    8 workflow.go()

**File Patterns**
~~~~~~~~~~~~~~~~~

A set of files can also be a dependency. For example, if we only wanted to list all global executables that started with the letter "a", we could add them all as a dependency that is a set of files.

:: 

    1 from anadama2 import Workflow
    2 from anadama2.tracked import TrackedFilePattern
    3 workflow = Workflow(remove_options=["input","output"])
    4 workflow.add_task("ls /usr/bin/a* | sort > [targets[0]]", targets="global_exe.txt", depends=TrackedFilePattern("/usr/bin/a*"))
    5 workflow.add_task("ls $HOME/.local/bin/ | sort > [targets[0]]", targets="local_exe.txt")
    6 workflow.add_task("join [depends[0]] [depends[1]] > [targets[0]]", depends=["global_exe.txt","local_exe.txt"], targets="match_exe.txt")
    7 workflow.go()

**Pre-Existing Dependencies**
-----------------------------

Tasks that depend on items that are not created by other tasks have pre-existing dependencies. By default AnADAMA2 will automatically try to register the pre-existing dependencies. If the dependency does not exist at runtime, an error will be issued. For example, if the file "global_exe.txt" did not exist and the following workflow was run an error will be issued.

:: 

    1 from anadama2 import Workflow
    2
    3 workflow = Workflow(remove_options=["input","output"],strict=True)
    4 workflow.do("ls $HOME/.local/bin/ | sort > [t:local_exe.txt]")
    5 workflow.do("join [d:global_exe.txt] [d:local_exe.txt] > [t:match_exe.txt]")
    6 workflow.go()

There is a function that can register these dependencies. To use this function we would add line four to the prior workflow example.

:: 

    1 from anadama2 import Workflow
    2
    3 workflow = Workflow(remove_options=["input","output"],strict=True)
    4 workflow.already_exists("global_exe.txt")
    5 workflow.do("ls $HOME/.local/bin/ | sort > [t:local_exe.txt]")
    6 workflow.do("join [d:global_exe.txt] [d:local_exe.txt] > [t:match_exe.txt]")
    7 workflow.go()


**API Documentation**
---------------------

The API documentation is hosted by Read the Docs at: `http://anadama2.readthedocs.io/ <http://anadama2.readthedocs.io/>`_ 

The API is automatically generated and will reflect the latest codebase.

If you would like to generate your own copy of the API, run the following command from the AnADAMA2 folder:

``$ python setup.py sphinx_build``
