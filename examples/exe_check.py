from anadama import Workflow

workflow = Workflow()
workflow.do("ls /usr/bin/ | sort > @{global_exe.txt}")
workflow.do("ls $HOME/.local/bin/ | sort > @{local_exe.txt}")
workflow.do("join #{global_exe.txt} #{local_exe.txt} > @{match_exe.txt}")
workflow.go()
