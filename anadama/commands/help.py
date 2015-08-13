from operator import attrgetter

import six

from doit.exceptions import InvalidCommand, InvalidDodoFile
from doit.cmd_base import DoitCmdBase
from doit.cmd_help import Help as DoitHelp

from ..loader import PipelineLoader
from ..util.help import print_pipeline_help

class Help(DoitHelp):
    name = "help"

    @staticmethod
    def print_usage(cmds):
        """Print anadama usage instructions"""
        print("AnADAMA -- https://bitbucket.org/biobakery/anadama")
        print('')
        print("Commands")
        for cmd in sorted(six.itervalues(cmds), key=attrgetter('name')):
            six.print_("  anadama %s \t\t %s" % (cmd.name, cmd.doc_purpose))
        print("")
        print("  anadama help                              show help / reference")
        print("  anadama help task                         show help on task fields")
        print("  anadama help pipeline <module.Pipeline>   show module.Pipeline help")
        print("  anadama help <command>                    show command usage")
        print("  anadama help <task-name>                  show task usage")


    def execute(self, params, args):
        """execute cmd 'help' """
        cmds = self.doit_app.sub_cmds
        if len(args) == 0 or len(args) > 2:
            self.print_usage(cmds)
        elif args[0] == 'task':
            self.print_task_help()
        elif args == ['pipeline']:
            six.print_(cmds['pipeline'].help())
        elif args[0] == 'pipeline':
            cls = PipelineLoader._import(args[1])
            print_pipeline_help(cls)
        elif args[0] in cmds:
            # help on command
            six.print_(cmds[args[0]].help())
        else:
            # help of specific task
            try:
                if not DoitCmdBase.execute(self, params, args):
                    self.print_usage(cmds)
            except InvalidDodoFile as e:
                self.print_usage(cmds)
                raise InvalidCommand("Unable to retrieve task help: "+e.message)
        return 0


