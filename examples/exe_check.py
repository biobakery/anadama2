from anadama2 import Workflow

workflow = Workflow()
workflow.do("ls /usr/bin/ | sort > [t:global_exe.txt]")
workflow.do("ls $HOME/.local/bin/ | sort > [t:local_exe.txt]")
workflow.do("join [d:global_exe.txt] [d:local_exe.txt] > [t:match_exe.txt]")
workflow.go()
