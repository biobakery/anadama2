import anadama.deps
from anadama import RunContext

ctx = RunContext()
step1_const = anadama.deps.KVContainer(a = 20)
step1 = ctx.add_task("echo {depends[0]} > {targets[0]}",
                     depends=step1_const.a, targets="step1.txt",
                     name="Step 1")

step2 = ctx.add_task("p=$(cat {depends[0]}); echo $p > {targets[0]}",
                     depends=step1.targets[0], targets="step2.txt",
                     name="Step 2")
ctx.go()
