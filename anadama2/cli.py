import re
import sys
import logging
import optparse

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
    "output": optparse.make_option("-o", '--output', default=script_wd(), type="str",
                         help="""Store output in this directory. By default the 
                         dependency database and run log are also put in 
                         this directory"""),
    "input": optparse.make_option("-i", '--input', default=script_wd(), type="str",
                         help="Collect inputs from this directory. "),
    "dry_run": optparse.make_option("-d", '--dry-run', action="store_true",
                         help="Print tasks to be run but don't execute their actions."),
    "run_them_all": optparse.make_option("-a", '--run-them-all', action="store_true",
                         help="Skip no tasks; run it all."),
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
    "exclude_target": optparse.make_option("-T", "--exclude_target", default=[], action="append",
                         help="""Don't execute tasks that make these targets.  
                         Use this flag multiple times to exclude many targets. If the 
                         provided value includes `?' or `*' or `[', treat it as
                         a pattern and exclude all targets that match.""")
}

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
        self.version = version
        #: args is a list containing any positional arguments passed at
        #: the command line. Think sys.argv.
        self.args = []
        self.namespace = namespace if namespace is not None else script_wd()

        self._directives = {}
        self._shorts = set()
        self._directories = {}
        self._user_asked = False
        self._callbacks = {}
        self._tracked = set()

        if not description:
            optparse.OptionParser.format_description = lambda s, t: s.description
            self.description = BANNER
        self.parser = optparse.OptionParser(
            description=self.description,
            version=self.version or "%prog v1.0.0"
        )
        if defaults:
            self._directives.update(default_options)
            for opt in default_options.values():
                self._shorts.add(opt._short_opts[0][1])
            for name in _default_dir:
                self._directories[name] = self._directives[name].default


    def add(self, name, desc=None, type="str", default=None, short=None,
            callback=None, tracked=False):
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

        :returns: self (the current Configuration object)

        """
        if callback:
            self._callbacks[name] = callback
            type="bool"
        type, action = self._reg_type(type, name, default)
        d = optparse.make_option(self._find_short(name, short),
                                 "--"+kebab(name), help=desc,
                                 type=type, action=action, default=default)
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

        return getattr(self, name, default)


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
    
