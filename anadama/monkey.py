
def patch_CmdParse_parse():
    from doit.cmdparse import CmdParse 
    from anadama.cmdparse import parse

    CmdParse.parse = parse


def patch_Action_actions():
    import doit.action
    from anadama.action import CmdAction, PythonAction

    doit.action.CmdAction = CmdAction
    doit.action.PythonAction = PythonAction


ALL = [patch_CmdParse_parse, patch_Action_actions]

def patch_all():
    for patcher_func in ALL:
        patcher_func()
