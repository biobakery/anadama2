import anadama2.tracked
from anadama2 import Workflow

workflow = Workflow(remove_options=["input","output"])

# create a container class to track
container = anadama2.tracked.Container(a = 20)

# add a task that depends on the "a" variable in the container
task1=workflow.add_task(
    "echo [depends[0]] > [targets[0]]",
    depends=container.a, 
    targets="echo.txt",
    name="task1")

# add a task that depends on the targets of task1 
task2=workflow.add_task(
    "p=$(cat [depends[0]]); echo $p > [targets[0]]",
    depends=task1.targets[0],
    targets="echo2.txt",
    name="task2")

workflow.go()

