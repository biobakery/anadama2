#!/usr/bin/env python

import sys

from doit.doit_cmd import DoitMain
from doit.cmd_base import DoitCmdBase

from . import commands
from . import monkey

class Main(DoitMain):
    def __init__(self, extra_cmds, *args, **kwargs):
        self._extra_cmds = extra_cmds
        super(Main, self).__init__(*args, **kwargs)
    
    def get_commands(self):
        cmds = DoitMain.get_commands(self)
        for extra_cmd_cls in self._extra_cmds:
            if issubclass(extra_cmd_cls, DoitCmdBase):
                extra_cmd = extra_cmd_cls(task_loader=self.task_loader)
                extra_cmd.doit_app = self
            else:
                extra_cmd = extra_cmd_cls()
            cmds[extra_cmd.name] = extra_cmd
        return cmds


def main(cmds=commands.all):
    monkey.patch_all()
    ret = Main(cmds).run(sys.argv[1:])
    sys.exit(ret)

if __name__ == '__main__':
    main()
