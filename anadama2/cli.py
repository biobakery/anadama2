# -*- coding: utf-8 -*-
import re
import sys
import logging
import optparse
import os

from .tracked import TrackedVariable
from .util import kebab, Directory
from .util.fname import script_wd

BANNER = """
                          _____          __  __
     /\             /\   |  __ \   /\   |  \/  |   /\    \\
====/  \===_=__====/  \==| |  | |=/  \==| \  / |==/  \====\\
===/ /\ \=| '_ \==/ /\ \=| |  | |/ /\ \=| |\/| |=/ /\ \===//
  / ____ \| | | |/ ____ \| |__| / ____ \| |  | |/ ____ \ //
 /_/    \_\_| |_/_/    \_\_____/_/    \_\_|  |_/_/    \_\

"""

default_options = {
    "output": optparse.make_option("-o", '--output', default=None, type="str",
                         help="""Store output in this directory. By default the 
                         dependency database and run log are also put in 
                         this directory"""),
    "input": optparse.make_option("-i", '--input', default=os.getcwd(), type="str",
                         help="Collect inputs from this directory. "),
    "dry_run": optparse.make_option("-d", '--dry-run', action="store_true",
                         help="Print tasks to be run but don't execute their actions."),
    "grid_run": optparse.make_option("-g", '--grid-run',
                         help="""Run gridable tasks on the grid provided. If not set,
                         then all tasks are run locally (including gridable tasks).
                         Grid type followed by partition should be provided.
                         For example 'slurm general_queue'""",
                         nargs=2),
    "skip_nothing": optparse.make_option("-n", '--skip-nothing', action="store_true",
                         help="Skip no tasks, even if you could; run it all."),
    "quit_early": optparse.make_option("-e", '--quit-early', action="store_true",
                         help="""If any tasks fail, stop all execution immediately. If set to
                         ``False`` (the default), children of failed tasks are *not*
                         executed but children of successful or skipped tasks *are*
                         executed: basically, keep going until you run out of tasks
                         to execute."""),
    "jobs": optparse.make_option("-j", '--jobs', default=1, type=int,
                         help="The number of tasks to execute in parallel locally."),
    "grid_jobs": optparse.make_option("-J", '--grid-jobs', default=1, type=int,
                         help="The number of tasks to submit to the grid in parallel."),
    "until_task": optparse.make_option("-u", '--until-task', default=None,
                         help="""Stop after running the named task. Can refer to
                         the end task by task number or task name."""),
    "exclude_task": optparse.make_option("-U", "--exclude-task", default=[], action="append",
                         help="""Don't execute these tasks. Use this flag multiple times 
                         to not execute many tasks"""),
    "target": optparse.make_option("-t", "--target", default=[], action="append",
                         help="""Only execute tasks that make these targets.  
                         Use this flag multiple times to build many targets. If the 
                         provided value includes `?' or `*' or `[', treat it as
                         a pattern and build all targets that match."""),
    "exclude_target": optparse.make_option("-T", "--exclude-target", default=[], action="append",
                         help="""Don't execute tasks that make these targets.  
                         Use this flag multiple times to exclude many targets. If the 
                         provided value includes `?' or `*' or `[', treat it as
                         a pattern and exclude all targets that match.""")
}

# a list of the required options (as optparse does not have a required keyword)
# these options must be supplied by the user on the command line or
# the workflow must set the option default to a value using the change function
required_options = ["output"]

_default_dir = ("output","input")


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
    
    def __init__(self, description=None, version=None, defaults=True,
                 namespace=None):
        self.description = description
        self.version = version or "0.9"
        #: args is a list containing any positional arguments passed at
        #: the command line. Think sys.argv.
        self.args = []
        self.namespace = namespace if namespace is not None else script_wd()

        self._directives = {}
        # Add the help short option so it is not selected by the function _find_short
        # when trying to find the short option for a new user option
        self._shorts = set('h')
        self._directories = {}
        self._user_asked = False
        self._callbacks = {}
        self._tracked = set()
        self._required_options = []

        if not description:
            optparse.OptionParser.format_description = lambda s, t: s.description
            self.description = BANNER
        self.parser = optparse.OptionParser(
            description=self.description,
            version="%prog v" + self.version
        )
        if defaults:
            self._directives.update(default_options)
            self._required_options=required_options
            for opt in default_options.values():
                self._shorts.add(opt._short_opts[0][1])
            for name in _default_dir:
                self._directories[name] = self._directives[name].default


    def add(self, name, desc=None, type="str", default=None, short=None,
            callback=None, tracked=False, required=None):
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

        :keyword type: Transform values for this option into a
          particular type. Acceptable types include the types
          understood by optparse. Additionally, "bool" and "dir" types
          are accepted. Bool types store boolean options (True or
          False). Dir types store objects of type
          :class:`anadama2.util.Directory`. If any option of type "dir"
          is added, an additional option ``--deploy`` is added to the
          Configuration object. The ``--deploy`` option creates the
          directories specified. Default: str.
        :type type: str
        
        :keyword default: Set a default for the option. This value is
          stored if the user doesn't set the flag for this option.

        :keyword short: Set the short flag for this option. Defaults
          to find a short flag based off of the value for ``name``.
        :type short: str
        
        :keyword callback: Set a function to execute when the user
          sets the flag for this option.
        :type callback: callable
        
        :keyword required: Add this to the list of required options.

        :returns: self (the current Configuration object)

        """
        
        # remove special characters from the name
        name=kebab(name)
        
        if callback:
            self._callbacks[name] = callback
            type="bool"
        type, action = self._reg_type(type, name, default)
        d = optparse.make_option(self._find_short(name, short),
                                 "--"+name, help=desc,
                                 type=type, action=action, default=default)
        
        # replace dash with underscore for name (as this will be done by optparse)
        name=re.sub(r'-', '_',name)
        if required:
            self._required_options.append(name)

        self._directives[name] = d
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


        :returns: self (the current Configuration object)

        """
        if 'default' in kwargs:
            self._directives[name].default = kwargs['default']
        return self


    def remove(self, name):
        """ Remove an option from the Configuration object.
        :param name: The name of the option to remove

        :returns: self (the current Configuration object)

        """
        self._shorts.remove(self._directives[name]._short_opts[0][1])
        del self._directives[name]
        self._directories.pop(name, None)
        self._callbacks.pop(name, None)
        if name in self._tracked:
            self._tracked.remove(name)
        return self


    def get(self, name, default=None):
        """Get a stored option value from the Configuration object.
        
        :param name: The name of the value to get
        """
        
        # get arguments from the command line (will not run again if already parsed)
        if not self._user_asked:
            self.ask_user()

        return getattr(self, name, default)
    
    def get_option_values(self):
        """Get all of the stored option values. Replace any custom class
        instances with their corresponding values."""
        
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
        for option in self._directives:
            option = re.sub(r'-', '_', option)
            if isinstance(option, Directory):
                value=self.get(option).name
            elif isinstance(option, TrackedVariable):
                value=self.get(option).__str__()
            else:
                value=self.get(option)
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
        if self._user_asked and not override:
            return self
        if self._directories and 'deploy' not in self._callbacks:
            self.add("deploy", desc="Create directories used by other options",
                     callback=self._deploy, type="bool")
        for opt in self._directives.values():
            self.parser.add_option(opt)
        opts, self.args = self.parser.parse_args(args=argv)
        for name in self._directives:
            name = re.sub(r'-', '_', name)
            val = getattr(opts, name)
            # if this is a required option, check that it has been provided
            # by the user on the command line or that the default was set
            # in the workflow
            if name in self._required_options and not val:
                self.parser.error("the "+str(self._directives[name])+" option is required")
            if name in self._directories:
                val = Directory(val)
            if name in self._tracked:
                val = TrackedVariable(self.namespace, name, val)
            if name in self._callbacks and val is True:
                self._callbacks[name](val)
            logger.info("Configured variable `%s' : `%s'", name, val)
            setattr(self, name, val)
        self._user_asked = True
        return self

        
    def _find_short(self, name, short=None):
        s = None
        if short and short[-1] not in self._shorts:
            s = short[-1]
        for char in name.lower():
            if 96 < ord(char) < 123 and char not in self._shorts:
                s = char
                break
        if s is None:
            raise ValueError("Unable to find short option flag for "+name)
        self._shorts.add(s)
        return "-"+s


    def _reg_type(self, t, name, dirname=None):
        if t in ("dir", dir, "directory"):
            self._directories[name] = dirname
            return "string", "store"
        if t in ("bool", bool):
            return None, "store_true"
        return t, "store"

        
    def _deploy(self):
        for d in self._directories.values():
            Directory(d).create()
            print >> sys.stderr, "created directory: "+d
        sys.exit(0)
    
