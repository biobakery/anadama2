#!/usr/bin/env python

import sys

from doit.doit_cmd import DoitMain

from . import commands

class Main(DoitMain):
    
    def __init__(self, extra_cmds, *args, **kwargs):
        self._extra_cmds = extra_cmds
        super(Main, self)__init__(*args, **kwargs)
    
    def get_commands(self):
        cmds = DoitMain.get_commands(self)
        for extra_cmd_cls in self._extra_cmds:
            extra_cmd = extra_cmd_cls()
            cmds[extra_cmd.name] = extra_cmd
        return cmds

def main(cmds=commands.all):
    ret = Main(cmds).run(sys.argv[1:])
    sys.exit(ret)

if __name__ == '__main__':
    main()
