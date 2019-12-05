# -*- coding: utf-8 -*-
import re
import sys
import logging
import argparse
import os
import subprocess
import collections

from .util import kebab
from .util.fname import script_wd

logger = logging.getLogger(__name__)

class Configuration(object):
    """The Configuration class makes objects that get user input via a
    command line interface and store the user input for easy access.

    Typical usage is as follows:

    .. code:: python

        from anadama2.cli import Configuration

        conf = Configuration(
            "StrainPhlan workflow"
        ).add("input", desc="Input Fastq files", type="dir",
              default="input/", pattern="*.fastq"
        ).add("output", type="dir",
              default="output/"
        ).ask_user()

        conf.input # "input/"
        conf.output # "output/"


    :keyword description: Set the description shown to the user when
      the help flag is given
    :type description: str

    :keyword version: Set the version shown to the user when the
      version flag is given
    :type version: str

    :keyword defaults: Add a set of default options to the
      Configuration object. These defaults change behavior of
      :meth:`anadama2.workflow.Workflow.go`. 
    :type defaults: bool

    """
    
    def __init__(self, description=None, version=None, defaults=True, remove_options=None, prompt_user=True):
        # set the description and version for the workflow
        self.description = description
        self.version = version
        
        # set if should prompt user for command line arguments
        self.prompt_user=prompt_user

        self._arguments = {}
        self._user_arguments = collections.OrderedDict()

        # Add the help short option so it is not selected by the function _find_short
        # when trying to find the short option for a new user option
        self._shorts = set('h')
        self._user_asked = False

        if not description:
            self.description = "AnADAMA2 Workflow"
            
        # create a parser instance
        self.parser = argparse.ArgumentParser(
            description=self.description,
            formatter_class=argparse.RawTextHelpFormatter)
        
        if defaults:
            self._arguments=collections.OrderedDict(self.get_default_options())
            for opt, info in self.get_default_options():
                self._shorts.add(info.short)

        # remove default options, if provided
        if remove_options:
            for option in remove_options:
                self.remove(option)
                
        # add a version option if version is provided
        if self.version:
            self.parser.add_argument("--version", action="version", version="%(prog)s v"+self.version)
            
            
    @staticmethod
    class Argument(object):
        def __init__(self, short, long, default=None, type=None, action=None, 
            choices=None, help=None, dest=None, required=None):
            
            # set the short and long option name
            self.short=short
            self.long=long
            
            # keep the keywords for the option that are set
            keywords={"default":default, "type":type, "action":action, "choices":choices, "help":help, "dest":dest, "required":required}
            self.keywords = {key:value for key, value in keywords.items() if value is not None}
                
    @classmethod
    def get_default_options(cls):
        return [
            ("output", cls.Argument("-o", "--output", required=True, 
                help="Write output to this directory")),
            ("input", cls.Argument("-i", "--input", default=os.getcwd(), 
                help="Find inputs in this directory \n[default: %(default)s]")),
            ("jobs", cls.Argument(None, "--local-jobs", default=1, type=int, dest="jobs", 
                help="Number of tasks to execute in parallel locally \n[default: %(default)s]")),
            ("grid_jobs", cls.Argument(None, '--grid-jobs', default=0, type=int, 
                help="Number of tasks to execute in parallel on the grid \n[default: %(default)s]")),
            ("grid", cls.Argument(None, "--grid", default=cls.identify_grid(),
                help="Run gridable tasks on this grid type \n[default: %(default)s]")),
            ("grid_partition", cls.Argument(None, "--grid-partition", default=cls.default_partitions(),
                help="Partition/queue used for gridable tasks.\nProvide a single partition or a comma-delimited list\nof short/long partitions with a cutoff.\n[default: %(default)s]")),
            ("grid_benchmark", cls.Argument(None, "--grid-benchmark", default="on" if cls.identify_grid() != "None" else "off", choices=["on","off"],
                help="Benchmark gridable tasks \n[default: %(default)s]")),
            ("grid_options", cls.Argument(None, "--grid-options", action="append",
                help="Grid specific options that will be applied to each grid task")),
            ("grid_environment", cls.Argument(None, "--grid-environment", action="append",
                help="Commands that will be run before each grid task to set up environment")),
            ("dry_run", cls.Argument(None, "--dry-run", action="store_true", 
                help="Print tasks to be run but don't execute their actions ")),
            ("skip_nothing", cls.Argument(None, "--skip-nothing", action="store_true", 
                help="Run all tasks. Rerun tasks that have already been run.")),
            ("quit_early", cls.Argument(None, '--quit-early', action="store_true",
                help="Stop if a task fails. By default,\nall tasks (except sub-tasks of failed tasks) will run.")),
            ("until_task", cls.Argument(None, "--until-task",
                help="Stop after running this task. Use task name or number.")),
            ("exclude_task", cls.Argument(None, "--exclude-task", action="append",
                help="Don't run these tasks. Add multiple times to append.")),
            ("target", cls.Argument(None, "--target", action="append",
                help="Only run tasks that generate these targets.\nAdd multiple times to append.\nPatterns with ? and * are allowed.")),
            ("exclude_target", cls.Argument(None, "--exclude-target", action="append",
                help="Don't run tasks that generate these targets.\nAdd multiple times to append.\nPatterns with ? and * are allowed.")),
            ("log_level", cls.Argument(None,"--log-level",default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], 
                help="Set the level of output for the log \n[default: %(default)s]"))
        ]
                
    @staticmethod
    def identify_grid():
        """ Check if a grid is found on the machine and determine which type """
        
        found_grid="None"
        # check for the grid job submission command for slurm and sge
        for command, grid in [["sbatch","slurm"],["qsub","sge"],["aws","aws"]]:
            try:
                output=subprocess.check_output(["which",command],stderr=subprocess.STDOUT)
                found_grid=grid
            except subprocess.CalledProcessError:
                pass
            
        return found_grid
    
    @classmethod
    def default_partitions(cls):
        """ Get default partitions based on the default grid """
        
        grid = cls.identify_grid()
        if grid == "slurm":
            partitions = ["serial_requeue","shared",4*60]
        elif grid == "sge":
            partitions = "broad"
        elif grid == "aws":
            partitions = ["general"]
        else:
            partitions = []
            
        return ",".join(map(str,partitions))

    def add(self, name, desc=None, type=None, default=None, short=None,
            action=None, choices=None, required=None, add_short=None, add_default=None):
        """Add an option to the Configuration object.

        :param name: Set the name of the option. This name is
          transformed with :func:`anadama2.util.kebab` to the long
          option flag in the command line interface e.g. ``entry_point
          -> --entry-point``.
        :type name: str

        :keyword desc: Set the description of the option. This
          description is shown next to the option name in the help
          display shown when the user gives the ``--help`` flag.
        :type desc: str

        :keyword type: Argparse type
        :type type: str
        
        :keyword default: Set a default for the option. This value is
          stored if the user doesn't set the flag for this option.

        :keyword short: Set the short flag for this option. Defaults
          to find a short flag based off of the value for ``name``.
        :type short: str
        
        :keyword action: Set an action to execute with flag
        :type action: function or string
        
        :keyword required: Add this to the set of required options.

        :returns: self (the current Configuration object)

        """
        
        # remove special characters from the name
        name=kebab(name)
        
        # change format of final name
        option = re.sub(r'-', '_', name)
        
        # compute the short if requested
        if add_short:
            short=self._find_short(name,short)
            
        if short:
            short="-"+short
            
        # add the default to the option
        if desc and default is not None:
            desc=desc+"\n[default: %(default)s]"

        # add the new item to the end of the user arguments
        self._user_arguments[option] = self.Argument(short, "--"+name, default, type, action, 
            choices, help=desc, required=required)

        return self

    def change(self, name, **kwargs):
        """Change an option. Use keyword options to change the attribute of
        the option. 

        Here's an example:

        .. code:: python

            from anadama2 import Workflow
            from anadama2.cli import Configuration

            ctx = Workflow(vars=Configuration().change("output", default="."))
            ...

        """
        
        for key, value in kwargs.items():
            self._arguments[name][key]=value

    def remove(self, name):
        """ Remove an option from the Configuration object.
        :param name: The name of the option to remove
        
        """
        
        # remove the short if set
        try:
            self._shorts.remove(self._arguments[name].short)
        except KeyError:
            pass
        
        del self._arguments[name]


    def get(self, name, default=None):
        """Get a stored option value from the Configuration object.
        
        :param name: The name of the value to get
        """
        
        # get arguments from the command line (will not run again if already parsed)
        if not self._user_asked:
            self.ask_user()

        return getattr(self, name, default)
    
    def get_option_values(self):
        """Get all of the values for the command line options. Prompt user if not asked."""
        
        class CommandLineOptions(object):
            def __getattr__(self, name):
                # if an attribute can not be found, this is the last function called
                all_option_names=", ".join(vars(self).keys())
                error_message="Unable to find option '{0}' in command line options.\n".format(name)
                error_message+="The available options are: {0}".format(all_option_names)
                raise AttributeError(error_message)
            
        # get arguments from the command line (will not run again if already parsed)
        if not self._user_asked:
            self.ask_user()
        
        args=CommandLineOptions()
        for option in list(self._user_arguments.keys()) + list(self._arguments.keys()):
            option = re.sub(r'-', '_', option)
            value = self.get(option)
            setattr(args,option,value)
                
        return args


    __getitem__ = get


    def reset(self):
        """Invalidate caching behavior of
        :meth:`anadama2.cli.Configuration.ask_user`. Use this method if
        you want to re-ask the user for input.

        """
        self._user_asked = False


    def ask_user(self, override=False, argv=None):
        """Set up and display the command line interface. Store defaults and
        any user input in the Configuration object. The defaults and
        user input are cached; if this method is called again, it does
        nothing and returns the current Configuration object.

        :keyword override: Override the caching behavior.
        :type override: bool

        :keyword argv: The command line arguments to parse. Defaults
          to sys.argv[1:].
        :type argv: list of str

        :returns: self (the current Configuration object)

        """
        # if not set to prompt user, then use defaults
        if not self.prompt_user:
            for arg_name, arg_values in self._arguments.items():
                setattr(self,arg_name,arg_values.keywords.get("default",None))
            return self
        
        if self._user_asked and not override:
            return self
        
        # add options by dictionary order starting with the user arguments
        for arg_name, arg_values in list(self._user_arguments.items()) + list(self._arguments.items()):
            if arg_values.short:
                self.parser.add_argument(arg_values.short, arg_values.long, **arg_values.keywords)
            else:
                self.parser.add_argument(arg_values.long, **arg_values.keywords)
            
        opts = self.parser.parse_args(args=argv)
        for name in list(self._user_arguments.keys()) + list(self._arguments.keys()):
            name = re.sub(r'-', '_', name)
            val = getattr(opts, name)
            logger.info("Command line argument `%s' = `%s'", name, val)
            setattr(self, name, val)
        self._user_asked = True
        return self

        
    def _find_short(self, name, short=None):
        s = None
        if short and short[-1] not in self._shorts:
            s = short[-1]
        for char in name.lower() + name.upper():
            char_ascii_value = ord(char)
            if 64 < char_ascii_value < 91 or 96 < char_ascii_value < 123 and char not in self._shorts:
                s = char
                break
        if s is None:
            raise ValueError("Unable to find short option flag for "+name)
        self._shorts.add(s)
        return "-"+s

    
