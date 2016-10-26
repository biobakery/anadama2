# -*- coding: utf-8 -*-
import anadama2.tracked
from anadama2 import Workflow

ctx = Workflow()
step1_const = anadama2.tracked.Container(a = 20)
step1 = ctx.add_task("echo [depends[0]] > [targets[0]]",
                     depends=step1_const.a, targets="step1.txt",
                     name="Step 1")

step2 = ctx.add_task("p=$(cat [depends[0]]); echo $p > [targets[0]]",
                     depends=step1.targets[0], targets="step2.txt",
                     name="Step 2")
ctx.go()
