import re
import sys
import optparse

from .util import kebab, Directory

BANNER = """
                          _____          __  __
     /\             /\   |  __ \   /\   |  \/  |   /\    \\
====/  \===_=__====/  \==| |  | |=/  \==| \  / |==/  \====\\
===/ /\ \=| '_ \==/ /\ \=| |  | |/ /\ \=| |\/| |=/ /\ \===//
  / ____ \| | | |/ ____ \| |__| / ____ \| |  | |/ ____ \ //
 /_/    \_\_| |_/_/    \_\_____/_/    \_\_|  |_/_/    \_\

"""

default_options = {
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
    "n_parallel": optparse.make_option("-n", '--n-parallel', default=1, type=int,
                         help="The number of tasks to execute in parallel locally."),
    "n_grid_parallel": optparse.make_option("-p", '--n-grid-parallel', default=1, type=int,
                         help="The number of tasks to submit to the grid in parallel."),
    "until_task": optparse.make_option("-u", '--until-task', default=None,
                         help="""Stop after running the named task. Can refer to
                         the end task by task number or task name."""),
}


class Configuration(object):
    def __init__(self, name=None, version=None, description=None, defaults=False):
        optparse.OptionParser.format_description = lambda s, t: s.description
        self.version = version
        self.description = description
        self.name = name
        self.args = []

        self._directives = {}
        self._shorts = set()
        self._directories = set()
        self._user_asked = False

        self.parser = optparse.OptionParser(
            description=self.description or BANNER,
            version=self.version or None
        )
        if defaults:
            self._directives.update(default_options)
            for opt in default_options.values():
                self._shorts.add(opt._short_opts[0][1])


    def add(self, name, desc=None, type="str", default=None, short=None):
        d = optparse.make_option(self._find_short(name, short),
                                 "--"+kebab(name), help=desc,
                                 type=self._reg_type(type, name), default=default)
        self._directives[name] = d
        return self


    def get(self, name):
        return getattr(self, name, None)


    def reset(self):
        self._user_asked = False


    def ask_user(self, override=False, argv=sys.argv[1:]):
        if self._user_asked and not override:
            pass
        for opt in self._directives.values():
            self.parser.add_option(opt)
        opts, args = self.parser.parse_args(argv)
        for name in self._directives:
            name = re.sub(r'-', '_', name)
            val = getattr(opts, name, None)
            if name in self._directories:
                val = Directory(val)
            setattr(self, name, val)
        self._user_asked = True
        return self

        
    def _find_short(self, name, short=None):
        s = None
        if short and short[0] not in self._shorts:
            s = short[0]
        for char in name.lower():
            if 96 < ord(char) < 123 and char not in self._shorts:
                s = char
                break
        if s is None:
            raise ValueError("Unable to find short option flag for "+name)
        self._shorts.add(s)
        return "-"+s


    def _reg_type(self, t, name):
        if t in ("dir", dir, "directory"):
            self._directories.add(name)
            return "string"
        return t
        
    
