# -*- coding: utf-8 -*-
import os
import re
import shlex
import fnmatch
import logging
import itertools
from operator import attrgetter, itemgetter
from collections import deque, defaultdict
import copy
import subprocess
import tempfile

import six
from six.moves import filter, map
import networkx as nx
from networkx.algorithms.traversal.depth_first_search import dfs_edges

from . import Task
from . import tracked
from . import grid as _grid
from . import reporters
from . import backends
from . import runners
from .cli import Configuration
from .taskcontainer import TaskContainer
from .helpers import format_command, build_actions
from .util import matcher, noop, find_on_path
from .util import istask, sugar_list, dichotomize
from .util import keepkeys
from .util import fname
from .grid.slurm import Slurm
from .grid.sge import SGE
from .grid.aws import AWS
from .document import PweaveDocument

second = itemgetter(1)
logger = logging.getLogger(__name__)


class RunFailed(ValueError):
    pass


class Workflow(object):
    """Create a Workflow.
    
    :keyword storage_backend: Lookup and save dependency information
      from this object. If ``None`` is passed (the default), the
      default backend from :func:`anadama2.backends.default` is used.
    :type storage_backend: instance of any
      :class:`anadama2.backends.BaseBackend` subclass or None.

    :keyword grid: Use this object to configure the run
      context to submit tasks to a compute grid.  
    :type grid: objects implementing the interface of
      :class:`anadama2.grid.Dummy
    
    :keyword strict: Enable strict mode. If strict, whenever a task is
      added that depends on something that is not the target of
      another task (or isn't marked with
      :math:`anadama2.workflow.Workflow.already_exists`), raise a
      ``KeyError``. If not strict, and the Tracked object
      ``.exists()`` automatically do what's necessary to track the
      object; if ``.exists()`` is False, raise a KeyError.
    :type strict: bool
    
    :keyword vars: Provide a custom Configuration class for command line options.
    :type vars: instance of any 
      :class:`anadama2.cli.Configuration` class or None.
      
    :keyword version: The version of the workflow. This version will be used for
      the command line option ``--version``.
    :type version: str
    
    :keyword description: A description of the workflow. This description 
      will be used in the command line ``--help`` message.
    :type description: str
    
    :keyword remove_options: A list of options to remove
    :type remove_options: list
    
    :keyword document: Provide a custom Document class.
    :type: document: instance of any :class: `anadama2.Document' class or None.
    """


    def __init__(self, storage_backend=None, grid=None, strict=False,
                 vars=None, version=None, description=None, remove_options=None,
                 document=None, cli=True):
        self.tmpdir = None
        self.task_counter = itertools.count()
        self.dag = nx.DiGraph()
        #: tasks is a :class:`anadama2.taskcontainer.TaskContainer`
        #: filled with objects of type :class:`anadama2.Task`. This
        #: list is populated as new tasks are added via
        #: :meth:`anadama2.workflow.Workflow.add_task` and
        #: :meth:`anadama2.workflow.Workflow.do`
        self.tasks = TaskContainer()
        #: task_results is a list of objects of type 
        #: :class:`anadama2.runners.TaskResult`. This list is populated
        #: only after tasks have been run with
        #: :meth:`anadama2.workflow.Workflow.go`.
        self.task_results = list()
        self._depidx = tracked.DependencyIndex()
        if grid:
            self.grid = grid
            self.grid_set = True
        else:
            self.grid = _grid.Dummy()
            self.grid_set = False
        self.strict = strict
        self.vars = vars or Configuration(description=description,
            version=version, defaults=True, remove_options=remove_options, prompt_user=cli)
        
        if document:
            self.document=document
        else:
            self.document=PweaveDocument()

        self._backend=None
        if storage_backend:
            self._backend = storage_backend

        logger.debug("Instantiated run context")

    def get_tmpdir(self):
        """Get the temp directory location. Called here as to not call argparse
        prior to custom args being added """      

        if self.tmpdir:
            return self.tmpdir
 
        # get the temp directory location
        self.tmpdir=self.vars.get("output")
        if self.tmpdir is None or tracked.s3_folder(self.tmpdir):
            # if no output folder is provided, then write to the current working directory
            self.tmpdir = os.getcwd()

        return self.tmpdir
 
    def _get_grid(self):
        """Return the grid instance. First check if a grid instance has already 
        been set or was provided initially. If not, set the grid based on the 
        command line options. A dummy grid is set if the option to run is not
        provided on the command line. In this case, all tasks will run locally."""
        
        # if the grid has already been set by the user, then return
        if self.grid_set:
            return self.grid
        
        # get the grid selection set on the command line
        grid_selection = self.vars.get("grid")
        grid_partition = self.vars.get("grid_partition")
        grid_jobs = self.vars.get("grid_jobs")
        grid_options = self.vars.get("grid_options")
        grid_environment = self.vars.get("grid_environment")
        grid_benchmark_setting = True if self.vars.get("grid_benchmark") == "on" else False
        
        # set the grid instance based on the user option
        # if no grid jobs are selected, then no grid will be used
        if grid_jobs == 0:
            grid = _grid.Dummy()
        elif grid_selection == "slurm":
            # get the temp output folder for the slurm scripts and stdout/stderr files
            tmpdir = os.path.join(self.get_tmpdir(), "slurm_files")
            grid = Slurm(partition=grid_partition, tmpdir=tmpdir, benchmark_on = grid_benchmark_setting,
                options=grid_options, environment=grid_environment)
        elif grid_selection == "sge":
            # get the temp output folder for the sge scripts and stdout/stderr files
            tmpdir = os.path.join(self.get_tmpdir(), "sge_files")
            grid = SGE(partition=grid_partition, tmpdir=tmpdir, benchmark_on = grid_benchmark_setting,
                options=grid_options, environment=grid_environment)
        elif grid_selection == "aws":
            # get the temp output folder for the aws scripts and stdout/stderr files
            tmpdir = os.path.join(self.get_tmpdir(), "aws_files")
            grid = AWS(partition=grid_partition, tmpdir=tmpdir)
        else:
            print("Grid selected ( "+grid_selection+" ) can not be found. Tasks will run locally.")
            grid = _grid.Dummy()
            
        self.grid=grid
        self.grid_set=True
        
        return self.grid
        

    def add_argument(self, name, **kwargs):
        """This function adds an argument to the configuration object provided
        to the workflow. Arguments can alternatively be added to the configuration
        object before it is provided to the workflow. See the ``add`` function
        documentation for your Configuration class, for more information. The 
        default configuration class is :class:`anadama2.cli.Configuration`."""
        
        self.vars.add(name, **kwargs)

    def parse_args(self):
        """Return the arguments parsed from the command line. Arguments are returned
        in the same format as calling argparse.parse_args(). An object with
        an attribute for each argument is returned. This custom object includes error
        reporting to aid the user in debugging when trying to get an argument
        that is not included in the list of command line arguments."""

        # return the arguments
        return self.vars.get_option_values()
    

    def get_input_files(self, extension=None, name=None, input_folder=None):
        """Return the files in the input folder filtered with the extension
        or name if provided. The input folder default can be set in the workflow
        or it can be provided on the command line by the user.
        
        :keyword extension: Return input files with this extension
        :type extension: str
        :keyword name: Return input files with this name
        :type name: str
        :keyword input_folder: Use this input folder instead of the default vars
        :type name: str

        :returns: A list of files
        """
        
        # get the contents of the input folder (with the full paths)
        if input_folder:
            vars_input = input_folder
        else:
            vars_input = self.vars.get("input")
        if tracked.s3_folder(vars_input):
            import boto3
            client = boto3.client("s3")
            bucket = tracked.s3_bucket(vars_input)
            paginator = client.get_paginator('list_objects')
            page_iterator = paginator.paginate(Bucket=bucket)

            contents = []
            for page in page_iterator:
                contents+= [i['Key'] for i in page['Contents']]
            input_files = [tracked.s3_build_path(bucket,file) for file in filter(lambda file: not file.endswith("/"), contents)]
        else:
            input = os.path.abspath(vars_input)
        
            input_folder_contents = map(lambda file: os.path.join(input, file), os.listdir(input))
            # filter out contents to only include files
            input_files = [item for item in input_folder_contents if os.path.isfile(item)]

        # if extension is set, then filter files
        if extension:
            input_files = list(filter(lambda file: file.endswith(extension), input_files))

        # if name is set, then filter files to only those with the exact name
        if name:
            input_files = list(filter(lambda file: os.path.basename(file) == name, input_files))
            
        return input_files
    
    def name_output_files(self, name, tag=None, extension=None, subfolder=None):
        """Return names of files in the output folder 
        use the name(s), tag, extension, and subfolder provided. 
        The output folder default can be set in the workflow or 
        it can be provided on the command line by the user.
        
        :parameter name: The name(s) of the output file(s)
        :type name: str or list
        :keyword tag: Add tag to the basename
        :type tag: str
        :keyword extension: Add extension to file name
        :type extension: str
        :keyword subfolder: Add the subfolder to the path to the file
        :type subfolder: str

        :returns: A list of file names
        """
        # if the name is a string, convert to list
        convert_to_string=False
        if not isinstance(name, list):
            name=[name]
            convert_to_string=True
       
        output_folder = self.vars.get("output") 
        if not tracked.s3_folder(output_folder):
            # get the output folder name (full path)
            output_folder = os.path.abspath(output_folder)

        # add the subfolder if provided
        if subfolder:
            output_folder = os.path.join(output_folder, subfolder)
            
        # Add tag and extension to file names
        output_files = [ fname.mangle(file, tag=tag, dir=output_folder, ext=extension) for file in name]
            
        # If the input was a single string, then convert output to single string
        if convert_to_string:
            output_files=output_files[0]
        
        return output_files

    def do(self, cmd, track_cmd=True, track_binaries=True):
        """Create and add a :class:`anadama2.Task` to the workflow using a
        convenient, shell-like syntax. 

        To explicitly mark task targets, wrap filenames within ``cmd``
        with ``[t:]``. Similarly, wrap dependencies with ``[d:]``. The
        literal ``[t:]`` and ``[d:]`` characters will be stripped out
        prior to execution by the shell.

        Below are some examples of using ``do``:

        .. code:: python

            from anadama2 import Workflow

            ctx = Workflow()
            ctx.do("wget -qO- checkip.dyndns.com > [t:my_ip.txt]")
            ctx.do(r"sed 's|.*Address: \(.*[0-9]\)<.*|\1|' [d:my_ip.txt] > [t:ip.txt]")
            ctx.do("whois $(cat [d:ip.txt]) > [t:whois.txt]")
            ctx.go()


        Variables from the workflow configuration can also be used
        inside ``cmd``. These are wrapped with ``[v:]``:

        .. code:: python

            from anadama2 import Workflow

            ctx = Workflow()
            ctx.do("wget -qO- checkip.dyndns.com > [v:output]/[t:my_ip.txt]")
            ctx.go()


        Modifiers inside the square brackets can be mixed and matched:

        .. code:: python

            from anadama2 import Workflow
            from anadama2.cli import Configuration

            ctx = Workflow(vars=Configuration().add("input", type="dir"))
            ctx.do("tar c [vd:input] | gzip -c > [t:output.tgz]")
            ctx.go()



        By default, changes made to ``cmd`` are tracked; any changes
        to ``cmd`` will cause this task to be rerun. Set ``track_cmd``
        to False to disable this behavior.

        Also by default, AnADAMA tries to discover pre-existing, small
        files in ``cmd`` and treat them as dependencies. This feature
        is intended to automatically track the scripts and binaries
        used in ``cmd``. Thus, this task will be re-run if any of the
        binaries or scripts change. Set ``track_binaries`` to False to
        disable this behavior.

        :param cmd: The shell command to add to the workflow. Wrap a
          target filename in ``[t:]`` and wrap a dependency filename
          in ``[d:]``. Variables from workflow configuration can be
          substituted into the command by wrapping the variable name
          in ``[v:]``. 
        :type cmd: str

        :keyword track_cmd: Set to False to not track changes to ``cmd``.
        :type track_cmd: bool
        
        :keyword track_binaries: Set to False to not discover files
          within ``cmd`` and treat them as dependencies.
        :type track_binaries: bool

        :returns: The :class:`anadama2.Task` just created

        """
        targs, ds = [], []

        def _repl(match):
            modifiers, name = match.groups()
            if 'v' in modifiers:
                name = self.vars.get(name) or _miss_exc(name)
            if 'd' in modifiers:
                ds.append(name)
            elif 't' in modifiers:
                targs.append(name)
            return str(name)

        sh_cmd = re.sub(r'\[([vdt]+):([^][]+)\]', _repl, cmd)
        if track_cmd:
            ns = os.path.abspath(tracked.Container.key(None))
            varname = "task_{}_command".format(len(self.tasks)+1)
            d = tracked.TrackedVariable(ns, varname, sh_cmd)
            ds.append(d)
        if track_binaries:
            to_preexist = []
            for binary in discover_binaries(cmd):
                to_preexist.append(binary)
                ds.append(binary)
            if to_preexist:
                self.already_exists(*to_preexist)

        return self.add_task(sh_cmd, depends=ds, targets=targs, name=sh_cmd,
                             interpret_deps_and_targs=False)


    def do_gridable(self, cmd, track_cmd=True, track_binaries=True, **gridopts):
        """Add a task to be launched on a grid computing system as specified
        in the ``grid`` option of
        :class:`anadama2.workflow.Workflow`. By default, this
        method is a synonym for
        :meth:`anadama2.workflow.Workflow.do`. Please see the
        ``add_task`` documentation for your powerup of choice
        e.g. :meth:`anadama2.slurm.Slurm.do` for information on
        options to provide to this method.
        """

        t = self.do(cmd, track_cmd, track_binaries)
        self._get_grid().do(t, **gridopts)
        return t
    
    def add_document(self, templates, depends=None, targets=None, vars=None, table_of_contents=None):
        """ Create and add a group of :class:`anadama2.Task` to the workflow. This
        task will create a document which will be the target(s) provided. The
        variables will be passed on to the template and be available when the
        document is generated from the template. The document class provided
        to the workflow will be used to create the document."""
        
        doc = copy.deepcopy(self.document)
        doc.__init__(templates, depends, targets, vars, table_of_contents)
        
        return self.add_task(actions=doc.create, depends=depends, targets=targets,
                      interpret_deps_and_targs=False, name="document")  
        
    def add_archive(self, depends, targets, archive_software=None, remove_log=None):
        """ Create an archive including the dependencies. Name it the target. This
        adds a :class:`anadama2.Task` to the workflow to create the archive. """

        # convert the depends and targets to lists if strings
        # update to absolute paths
        depends=sugar_list(depends)
        targets=[os.path.abspath(target) for target in sugar_list(targets)]
        
        # get the absolute path to the output folder
        if not self.vars.get("output") is None and not tracked.s3_folder(self.vars.get("output")):
            output_folder = os.path.abspath(self.vars.get("output"))
        else:
            output_folder = None
        
        # determine the archive software based on the target extension
        if archive_software is None:
            if targets[0].endswith(".zip"):
                archive_software = "zip"
            else:
                archive_software = "tar"
                
        # get the archive extension
        if archive_software == "zip":
            extension=".zip"
        else:
            extension=".tar"+targets[0].split("tar")[-1]
                
        # normalize a path to remove any extra separators included by the user
        # allows joining a folder ending with a separator with an extension as a target
        targets[0]=targets[0].replace(os.pathsep+extension,extension)
            
        # remove any tasks from the depends list of archive inputs
        archive_inputs=list(filter(lambda x: not isinstance(x,Task), depends))
        
        # if there is an output folder, change the depends to relative paths
        # so the full path is not included in the archive
        if output_folder is not None:
            archive_inputs=[os.path.relpath(file, start=output_folder) for file in archive_inputs]

        # check that the software is installed to create the archive
        try:
            output=subprocess.check_output([archive_software,"--help"])
        except EnvironmentError:
            raise EnvironmentError("Unable to archive document with software requested. Please install " +archive_software)
        
        # get the possible location of the db folder and the log
        document_db_folder="*"+backends.LOCAL_DB_FOLDER+"*"
        document_log=reporters.LOG_FILE_NAME
        
        # create the command always removing the database, if found
        # if the database is not found this will not issue an error it just will not be excluded
        if archive_software == "zip":
            command=["zip","-r",targets[0]]
            command+=archive_inputs
            command+=["-x",document_db_folder]
            if remove_log:
                command+=["-x",document_log]
        elif archive_software == "tar": 
            if targets[0].endswith(".tar.gz"):
                settings="czvf"
            elif targets[0].endswith(".tar"):
                settings="cvf"
            elif targets[0].endswith(".tar.bz2"):
                settings="cjvf"
            else:
                raise TypeError("Unknown archive software extension. Please use .tar.gz, .tar, or .tar.bz2 with tar software.")
                
            command=["tar",settings,targets[0]]
            command+=archive_inputs
            command+=["--exclude",document_db_folder]
            if remove_log:
                command+=["--exclude",document_log]
        else:
            raise TypeError("Unknown archive software provided. Please select zip or tar with .tar.gz or .tar or .tar.bz2")
        
        # move to the directory of the output folder to not include full paths in archives
        # if there is an output folder being used
        if output_folder is not None:
            cwd=os.getcwd()
            command=["cd",output_folder,";"]+command+[";","cd",cwd]
            
        command=" ".join(command)
        
        return self.add_task(actions=command, depends=depends, targets=targets,
                      interpret_deps_and_targs=False, name="archive")

    def add_task_group(self, actions=None, depends=None, targets=None,
                       name=[None], interpret_deps_and_targs=True, **kwargs):
        """Create and add a group of :class:`anadama2.Task` to the workflow. 
        This function will create a task for each set of depends and targets
        provided. The number of targets and dependencies should be the same.
        
        This function will call ``add_task`` for each task in the group. Please
        see the ``add_task`` documentation for more information."""
        
        task_group=[]
        for deps, targs, tname in zip(depends, targets, itertools.cycle(sugar_list(name))):
            task_group.append(self.add_task(actions, deps, targs, tname, interpret_deps_and_targs, **kwargs))
            
        return task_group
            
    def add_task_group_gridable(self, actions=None, depends=None, targets=None,
                       name=[None], interpret_deps_and_targs=True, **kwargs):
        """Create gridable tasks as a group."""
        
        task_group=[]
        for deps, targs, tname in zip(depends, targets, itertools.cycle(sugar_list(name))):
            task = self.add_task(actions, deps, targs, tname, interpret_deps_and_targs, **kwargs)
            task_group.append(task)
            self._get_grid().add_task(task, **kwargs)
        
        return task_group        

    def add_task(self, actions=None, depends=None, targets=None,
                 name=None, visible=True,
                 interpret_deps_and_targs=True, **kwargs):
        """Create and add a :class:`anadama2.Task` to the workflow.  This
        function can be used as a decorator to set a function as the
        sole action. 

        Extra keyword arguments can be used as formatting values
        similar to ``[depends[0]]``. See :func:`anadama2.helpers.parse_sh`
        
        :param actions: The actions to be performed to complete the
          task. Strings or lists of strings are interpreted as shell
          commands according to :func:`anadama2.helpers.parse_sh`. If given
          just a string or just a callable, this method treats it as a
          one-item list of the string or callable.
        :type actions: str or callable or list of str or
          list of callable

        :param depends: The dependencies of the task. The task must
          have these dependencies before executing the
          actions. Strings or lists of strings are interpreted as
          filenames and turned into objects of type
          :class:`anadama2.tracked.HugeTrackedFile`. If given just a string or just
          a :class:`anadama2.tracked.Base`, this method treats it as a
          one-item list of the argument provided.
        :type depends: str or :class:`anadama2.tracked.Base` or list of
          str or list of :class:`anadama2.tracked.Base`

        :param targets: The targets of the task. The task must produce
          these targets after executing the actions to be considered
          as "success". Strings or lists of strings are interpreted as
          filenames and turned into objects of type
          :class:`anadama2.tracked.HugeTrackedFile`. If given just a string or just
          a :class:`anadama2.tracked.Base`, this method treats it as a
          one-item list of the argument provided.
        :type targets: str or :class:`anadama2.tracked.Base` or list of
          str or list of :class:`anadama2.tracked.Base`

        :param name: A name for the task. Task names must be unique
          within a run context.
        :type name: str

        :keyword visible: Whether to show this task on the console. Set
          to ``False`` if it should only be in the debug log.
        :type visible: bool

        :keyword interpret_deps_and_targs: Should I use
          :func:`anadama2.helpers.parse_sh` to change
          ``[depends[0]]`` and ``[targets[0]]`` into the first item in
          depends and the first item in targets? Default is True 
        :type interpret_deps_and_targs: bool

        :returns: The :class:`anadama2.Task` just created

        """

        deps = _build_depends(depends)
        targs = _build_targets(targets)
        task_no = next(self.task_counter)
        if not actions: # must be a decorator
            def finish_add_task(fn):
                the_task = Task(name, [fn], deps, targs, task_no, bool(visible), [fn], kwargs, interpret_deps_and_targs)
                self._add_task(the_task)
            return finish_add_task
        else:
            # if any targets or depends generate temp files, create temp folder for task
            try:
                tracked_with_temp = list(filter(lambda x: x.temp_files(), deps+targs))
            except AttributeError:
                tracked_with_temp = []
            tmpdir = os.path.join(self.get_tmpdir(),"anadama2_temp_tracked")
            if visible and tracked_with_temp:
                if not os.path.isdir(tmpdir):
                    os.makedirs(tmpdir)
                tmpdir = tempfile.mkdtemp(dir=tmpdir)

                # set local temp locations for tracked items
                tmp = [tracked.try_set_local_path(x, tmpdir) for x in tracked_with_temp]

            acts = build_actions(actions, deps, targs, visible, kwargs, 
                                  use_parse_sh=interpret_deps_and_targs)

            the_task = Task(name, acts, deps, targs, task_no, bool(visible), actions, kwargs, interpret_deps_and_targs)
            self._add_task(the_task)
            return the_task


    def add_task_gridable(self, actions=None, depends=None, targets=None,
                      name=None, interpret_deps_and_targs=True, **gridopts):
        """Add a task to be launched on a grid computing system as specified
        in the ``grid`` option of
        :class:`anadama2.workflow.Workflow`. By default, this
        method is a synonym for
        :meth:`anadama2.workflow.Workflow.add_task`. Please see the
        ``add_task`` documentation for your powerup of choice
        e.g. :meth:`anadama2.grid.slurm.Slurm.add_task` for information on
        options to provide to this method.

        """
        if not actions: # must be a decorator
            def finish_grid_add_task(fn):
                t = self.add_task([fn], depends, targets, name)
                self._add_task(t)
                self.grid.add_task(t, **gridopts)
            return finish_grid_add_task
        else:
            t = self.add_task(actions, depends, targets, name,
                              interpret_deps_and_targs, **gridopts)
            self._get_grid().add_task(t, **gridopts)
            return t


    def already_exists(self, *depends):
        """Declare a dependency as pre-existing. That means that no task
        creates these dependencies; they're already there before any
        tasks run.

        .. note::

            If you have a list or other iterable containing the
            dependencies that already exist, you can declare them all
            like so ``ctx.already_exists(*my_bunch_of_deps)``.

        :param \*depends: One or many dependencies to mark as pre-existing.
        :type \*depends: any argument recognized by :func:`anadama2.tracked.auto`

        """

        self.add_task(noop, targets=list(map(tracked.auto, depends)),
                      name="Track pre-existing dependencies", visible=False)


    def go(self, skip_nothing=False, quit_early=False, runner=None,
           reporter=None, jobs=None, grid_jobs=None,
           until_task=None, exclude_task=None, target=None,
           exclude_target=None, dry_run=False):
        """Kick off execution of all previously configured tasks. 

        :keyword skip_nothing: Skip no tasks, even if you could.
        :type skip_nothing: bool
        
        :keyword quit_early: If any tasks fail, stop all execution
          immediately. If set to ``False`` (the default), children of
          failed tasks are *not* executed but children of successful
          or skipped tasks *are* executed: basically, keep going until
          you run out of tasks to execute.

        :keyword runner: The tasks to execute are passed to this
          object for execution.  For a list of runners that come
          bundled with anadama, see :mod:`anadama2.runners`. Passing
          ``None`` (the default) uses the default runner from
          :func:`anadama2.runners.default`.
        :type runner: instance of any
          :class:`anadama2.runners.BaseRunner` subclass or None.

        :keyword reporter: As task execution proceeds, events are
          dispatched to this object for reporting purposes. For more
          information of the reporters bundled with anadama, see
          :mod:`anadama2.reporters`. Passing ``None`` (the default)
          uses the default reporter from
          :func:`anadama2.reporters.default`.
        :type reporter: instance of any
          :class:`anadama2.reporters.BaseReporter` subclass or None.

        :keyword jobs: The number of tasks to execute in
          parallel. This option is ignored when a custom runner is
          used with the ``runner`` keyword.
        :type jobs: int

        :keyword grid_jobs: The number of tasks to submit to the
          grid in parallel. This option is ignored when a custom
          runner is used with the ``runner`` keyword. This option is
          also a synonym for ``jobs`` if the context has no grid
          powerup.
        :type grid_jobs: int

        :keyword until_task: Stop after running the named task. Can
          refer to the end task by task number or task name.
        :type until_task: int or str

        :keyword exclude_task: Don't execute this task or any of its
          children. Can refer to the task by task number or task name.
        :type exclude_task: int or str

        :keyword target: Execute the necessary tasks to produce this
          target. If ``target`` contains ``[``, ``*``, or ``?``, it is
          treated as a pattern and used to match multiple targets.
        :type target: str

        :keyword exclude_target: Don't execute any tasks that will
          produce this target. If ``target`` contains ``[``, ``*``, or
          ``?``, it is treated as a pattern and used to match multiple
          targets.
        :type exclude_target: str

        :keyword dry_run: Don't execute any actions, just say that you
          did.
        :type dry_run: bool

        """
        skip_nothing   = skip_nothing   or self.vars.get("skip_nothing")
        quit_early     = quit_early     or self.vars.get("quit_early")
        jobs           = jobs           or self.vars.get("jobs")
        grid_jobs      = grid_jobs      or self.vars.get("grid_jobs")
        until_task     = until_task     or self.vars.get("until_task")
        exclude_task   = exclude_task   or self.vars.get("exclude_task")
        target         = target         or self.vars.get("target")
        exclude_target = exclude_target or self.vars.get("exclude_target")
        dry_run        = dry_run        or self.vars.get("dry_run")

        self.completed_tasks = set()
        self.failed_tasks = set()
        self.task_results = [None for _ in range(len(self.tasks))]
        self._reporter = reporter or reporters.default(self.vars.get("output"),self.vars.get("log_level"))
        self._reporter.started(self)
        
        # if the backend is not set, then set to default
        if not self._backend:
            self._backend = backends.default(self.vars.get("output"))

        _runner = runner or self.grid.runner(self, jobs, grid_jobs)
        if dry_run:
            _runner = runners.DryRunner(self)
        _runner.quit_early = quit_early
        logger.debug("Sorting task_nos by network topology")
        task_idxs = nx.algorithms.dag.topological_sort(self.dag, reverse=True)
        logger.debug("Sorting complete")
        keep, drop = set(), set()
        if until_task:
            for task_name_or_no in sugar_list(until_task):
                for t in self._taskmatch(task_name_or_no):
                    keep = keep.union(allparents(self.dag, t.task_no))
        if exclude_task:
            for task_name_or_no in sugar_list(exclude_task):
                for t in self._taskmatch(task_name_or_no):
                    drop = drop.union(allchildren(self.dag, t.task_no))
        if target:
            for name_or_pattern in sugar_list(target):
                keep = self._targetmatch(keep, name_or_pattern, allparents)
        if exclude_target:
            for name_or_pattern in sugar_list(exclude_target):
                drop = self._targetmatch(drop, name_or_pattern, allchildren)
        if not keep:
            keep = set(task_idxs)
        task_idxs = list(filter((keep-drop).__contains__, task_idxs))
        if not skip_nothing:
            task_idxs = self._filter_skipped_tasks(task_idxs)
        task_idxs = deque(task_idxs)

        _runner.run_tasks(task_idxs)
        self._handle_finished()


    def _import(self, task_dict):
        keys_to_keep = ("actions", "depends", "targets",
                        "name", "interpret_deps_and_targs")
        return self.add_task(**keepkeys(task_dict, keys_to_keep))

    _ = _import


    def _handle_task_result(self, result):
        if result.task_no is not None:
            self.task_results[result.task_no] = result
        if result.error:
            self.failed_tasks.add(result.task_no)
            self._reporter.task_failed(result)
        else:
            self._backend.save(result.dep_keys, result.dep_compares)
            self.completed_tasks.add(result.task_no)
            self._reporter.task_completed(result)
            pxdeps = [ d for d in self.tasks[result.task_no].depends
                       if not istask(d) and d not in self._depidx ]
            if pxdeps:
                self._backend.save([d.name for d in pxdeps], 
                                   [list(d.compare()) for d in pxdeps])
                    


    def _handle_finished(self):
        self._reporter.finished()
        if self.failed_tasks:
            raise RunFailed()


    def _filter_skipped_tasks(self, task_idxs):
        should_run, idxs = dichotomize(task_idxs, self._always_rerun)
        should_run = set(should_run)
        for dep, idxs_set in self._aggregate_deps(idxs):
            if tracked.any_different([dep], self._backend):
                for idx in idxs_set:
                    logger.debug("Can't skip task %i because of dep change",
                                 idx)
                    should_run.add(idx)
        while idxs:
            idx = idxs.pop()
            if idx in should_run:
                continue
            for parent_idx in self.dag.predecessors(idx):
                if parent_idx in should_run:
                    should_run.add(idx)
                    logger.debug("Can't skip %i because it depends "
                                 "on task %i, which will be rerun",
                                 idx, parent_idx)

        to_run, skipped = dichotomize(task_idxs, should_run.__contains__)
        for idx in skipped:
            self._handle_task_skipped(idx)
        return to_run


    def _always_rerun(self, task_no):
        task = self.tasks[task_no]
        if not task.targets and not task.depends:
            logger.debug("Can't skip task %i because it "
                          "has no targets or depends", task_no)
            return True
        return False


    def _aggregate_deps(self, idxs):
        grp = defaultdict(set)
        for idx in idxs:
            task = self.tasks[idx]
            for dep in itertools.chain(task.depends, task.targets):
                if istask(dep):
                    continue
                grp[dep].add(idx)
                    
        return six.iteritems(grp)
        

    def _handle_task_started(self, task_no):
        self._reporter.task_started(task_no)


    def _handle_task_skipped(self, task_no):
        self.completed_tasks.add(task_no)
        self._reporter.task_skipped(task_no)

    def _add_task(self, task):
        """Actually add a task to the internal dependency data structure"""
        
        self.tasks.append(task)
        self.dag.add_node(task.task_no)
        for dep in task.depends:
            if istask(dep):
                self.dag.add_edge(dep.task_no, task.task_no)
                continue
            if dep in self._depidx:
                pass
            elif dep.must_preexist == False: 
                continue
            elif not self.strict and dep.exists():
                self.already_exists(dep)
            try:
                parent_task = self._depidx[dep]
            except KeyError:
                self._handle_nosuchdep(dep, task)
            else:
                # check to see if the dependency exists but doesn't
                # link to a task. This would happen if someone defined
                # a preexisting dependency
                if parent_task is not None:
                    self.dag.add_edge(parent_task.task_no, task.task_no)
        for targ in task.targets: 
            # add targets to the DependencyIndex after looking up
            # dependencies for the current task. Hopefully this avoids
            # circular references
            self._depidx.link(targ, task)
                

    def _handle_nosuchdep(self, dep, task):
        self.tasks.pop()
        self.dag.remove_node(task.task_no)
        msg = "Unable to find dependency `{}' of type `{}'. "
        msg = msg.format(str(dep), type(dep))
        alldeps = itertools.chain.from_iterable(
            [list(t.depends) + list(t.targets) for t in self.tasks]
        )
        try:
            closest = matcher.find_match(dep, alldeps, key=attrgetter("name"))
        except:
            raise KeyError(msg)
        msg += "Perhaps you meant `{}' of type `{}'?"
        raise KeyError(msg.format(str(closest), type(closest)))


    @property
    def _alltargets(self):
        return iter( (targ.name, task.task_no) for task in self.tasks
                     for targ in task.targets )

    def _targetmatch(self, s, name_or_pattern, hier, try_cwd=False):
        # try the target with the full path to the current working directory
        if try_cwd:
            name_or_pattern=os.path.join(os.getcwd(), name_or_pattern)

        if re.search(r'[?*\[]', name_or_pattern):
            regex = re.compile(fnmatch.translate(name_or_pattern))
            matches = [ no for name, no in self._alltargets
                        if regex.match(name) ]
            ret = set( sibling for match in matches
                       for sibling in hier(self.dag, match) )
            if not ret:
                msg = "Pattern {} matched no targets."
                if try_cwd:
                    msg+=" Tried without path and then with expected path. Please provide the full path."
                elif not name_or_pattern.startswith(os.sep):
                    return self._targetmatch(s, name_or_pattern, hier, try_cwd=True)
                raise ValueError(msg.format(name_or_pattern))
        else:
            try:
                match = next( iter(no for name, no in self._alltargets
                                   if name_or_pattern == name) )
            except StopIteration:
                msg = "Unable to find target {}.".format(name_or_pattern)
                if try_cwd:
                    msg+=" Tried without path and then with expected path. Please provide the full path."
                elif not name_or_pattern.startswith(os.sep):
                    return self._targetmatch(s, name_or_pattern, hier, try_cwd=True)
                raise ValueError(msg)
            ret = set(hier(self.dag, match))
        return s.union(ret)

    def _taskmatch(self, task_name_or_number):
        # Find the tasks that match the name or number provided
        # Multiple tasks can share the same name but all will have unique numbers
        
        taskset=[]
        for task in self.tasks:
            if task.name == task_name_or_number:
                taskset.append(task)
            elif task.task_no == task_name_or_number:
                taskset.append(task)
                
        return taskset

def _build_depends(depends):
    depends = filter(None, sugar_list(depends))
    return list(map(tracked.auto, depends))


def _build_targets(targets):
    targets = filter(None, sugar_list(targets))
    ret = list()
    for targ in targets:
        if istask(targ):
            raise ValueError("Can't make a task a target")
        ret.append(tracked.auto(targ))
    return ret


def _miss_exc(name):
    msg = "Unable to find configuration variable `{}'".format(name)
    raise Exception(msg)


def discover_binaries(s):
    """Search through string ``s`` and find all existing files smaller
    than 10MB. Return those files as a list of objects of type
    :class:`anadama2.tracked.TrackedExecutable`.
    """

    ds = list()
    for term in shlex.split(s):
        if not os.path.exists(term):
            term = find_on_path(term)
        if not term:
            continue
        if os.path.isdir(term):
            # don't want directories
            continue
        if not os.access(term, os.F_OK | os.X_OK):
            # doesn't exist or can't execute
            continue
        try:
            dep = tracked.TrackedExecutable(term)
        except ValueError:
            continue
        if os.stat(dep.name).st_size < 1<<20:
            ds.append(dep)

    return ds


def allchildren(dag, task_no):
    kids = map(second, dfs_edges(dag, task_no))
    return itertools.chain([task_no], kids)


def allparents(dag, task_no):
    seen = set()
    to_check = deque([task_no])
    while to_check:
        idx = to_check.popleft()
        if idx in seen:
            continue
        seen.add(idx)
        to_check.extend(dag.predecessors(idx))
    return seen
