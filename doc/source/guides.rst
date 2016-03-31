Introduction and Guides
#######################

.. toctree::
   :maxdepth: 2

   example_workflows


Introduction
############

AnADAMA is a python module for performing computations while managing
dependencies. You tell AnADAMA the tasks for your computation and tell
AnADAMA how the steps relate to each other.  AnADAMA then runs your
tasks in the right order, skipping any tasks that need not be
run. Should you need to scale your computation, AnADAMA can run your
tasks in parallel.

The Run Context
===============

You tell AnADAMA to perform your computation through a run context. It
acts as a container for your tasks, keeps track of task dependencies,
and ultimately runs your tasks.

To start, import the :class:`anadama.runcontext.RunContext` class from
the anadama module::

  >>> from anadama import RunContext

Now make a run context object::

  >>> ctx = RunContext()

The run context ``ctx`` will hold your workflow.


Adding Tasks
============

To actually get your workflow into the run context, use the
:meth:`anadama.runcontext.RunContext.add_task` or the
:meth:`anadama.runcontext.RunContext.do` methods.


``add_task()``
______________

This method creates an :class:`anadama.Task`, adds it to the run
context, and returns it to you for later use::

  >>> cmd = "wget http://www.gnu.org/licenses/gpl.html"
  >>> ctx.add_task(cmd, targets=["gpl.html"])
  Task(name='Step 0', actions=[<function actually_sh at 0x7fc5d8a9e938>], depends=[], targets=[<anadama.deps.FileDependency object at 0x7fc5d8f8dd90>], task_no=0)

We just added our first task to the run context: download the GPL from
gnu.org. We listed ``"gpl.html"`` as a target, which means that the
task will be marked as "failed" if running the task doesn't produce
the file ``gpl.html``.

.. note:: AnADAMA considers it an error to depend on a dependency
          that's not a target of some other task. If you need to
          depend on a dependency that already exists, use
          :meth:`anadama.runcontext.RunContext.already_exists`.

The :meth:`anadama.runcontext.RunContext.add_task` method returned us
the task it created. You can use this task in future tasks: use its
targets as dependencies downstream or use the entire task as a
dependency itself. There are a few things to note about the task
returned:

  * The ``actions`` attribute of the task is a list that holds just
    one function: ``<function actually_sh at 0x7fc5d8a9e938>``

  * The ``targets`` attribute is a list containing a
    :class:`anadama.deps.FileDependency`

  * The task has a name and a ``task_no``


Some of these should be surprising. We gave ``add_task`` a string for
an action, and it created a list containing a function.  This is
merely a convenience; AnADAMA makes the easy things easy. If you take
a look at the API reference for
:meth:`anadama.runcontext.RunContext.add_task`, you'll see that it
expects a list of functions for ``actions``, but will interpret
strings as shell commands and wrap that in a function that dispatches
commands to ``/bin/sh``. A similar thing happens for ``targets`` and
``depends``: :meth:`anadama.runcontext.RunContext.add_task` expects a
list of dependencies. Read more about dependencies at
:ref:`deps`. Finally, each task is given a name and a number. The
number is unique and refers to its spot in the ``tasks`` attribute of
a run context attribute. The name is just for cosmetic purposes; use
it to remember what that task does.

Function actions
----------------

Since :meth:`anadama.runcontext.RunContext.add_task` puts functions in
:class:`anadama.Task` ``actions``, let's put in some of our own::

  >>> def lis_only(task):
  ...     with open(str(task.depends[0])) as gpl, open(str(task.targets[0]), 'w') as out_f:
  ...         in_li = False
  ...         for line in gpl:
  ...             if '<li>' in line:
  ...                 in_li = True
  ...             elif '</li>' in line:
  ...                 in_li = False
  ...             if in_li:
  ...                 out_f.write(line)
  ... 
  >>> t2 = ctx.add_task(lis_only, depends="gpl.html", targets="lis_only.html")
  >>> t2
  Task(name='Step 1', actions=[<function lis_only at 0x7f6326e59848>], depends=[<anadama.deps.FileDependency object at 0x7fc5d8f8dd90>], targets=[<anadama.deps.FileDependency object at 0x7f6326e55ad0>], task_no=1)
	 
This function reads an input file line by line and only prints to an
output file when the line it's currently reading is in an ``<li>``
html element.  The task created depends on the ``gpl.html`` file and
produces a ``lis_only.html`` file.

When the run context runs this task, it'll call the function and pass
the current task as the function's only argument. We can use the task
to get input and output files. Remember that ``task.depends`` and
``task.targets`` are lists of :class:`anadama.deps.FileDependency`;
use ``str`` to use them as strings (otherwise ``open()`` will raise an
exception).

Function actions via decorator
------------------------------

:meth:`anadama.runcontext.RunContext.add_task` has a shortcut for
adding tasks with a single function action::

  >>> @ctx.add_task(depends=t2.targets, targets="changed.html", name="Change meaning")
  ... def change_meaning(task):
  ...     in_f = open(str(task.depends[0]))
  ...     out_f = open(str(task.targets[0]), 'w')
  ...     with in_f, out_f:
  ...         for line in in_f:
  ...             line = line.replace("permission", "bacon"
  ...                       ).replace("License",    "Hair Dryer")
  ...             out_f.write(line)
  ... 
  >>> ctx.tasks[2]
  Task(name='Change meaning', actions=[<function change_meaning at 0x7fab54d5b938>], depends=[<anadama.deps.FileDependency object at 0x7fab54d57ad0>], targets=[<anadama.deps.FileDependency object at 0x7fab54d57b90>], task_no=2)

Now we've added a task to the run context by decorator syntax.


``do()``
________

The :meth:`anadama.runcontext.RunContext.do` method is a shortcut for
adding tasks that are just one shell command. Instead of filling in
``depends=[...]`` and ``targets=[...]``, depends and targets are
defined by marking up the shell command with the following decoration:

  - To depend on a file, wrap the filename in your shell command with
    ``#{}``.

  - To mark a file as a target, wrap the filename in your shell
    command with ``@{}``.

Let's see an example::

    >>> from anadama import RunContext
    >>> 
    >>> ctx = RunContext()
    >>> t = ctx.do("wget -qO- checkip.dyndns.com > @{my_ip.txt}")
    >>> str(t.targets[0])
    '/home/rschwager/my_ip.txt'
  
In this task, we want to use ``wget`` to ask checkip.dyndns.com for
the my current IP address. Similar to
:meth:`anadama.runcontext.RunContext.add_task`,
:meth:`anadama.runcontext.RunContext.do` returns the task that was
just added to the run context. We can see that since we wrapped
``my_ip.txt`` in ``@{}``, the task that's returned has the
``my_ip.txt`` file listed as its first target.

Although the task defined above with ``.do()`` does not specify any
dependencies, it has two::

  >>> len(t.depends)
  2
  >>> map(str, t.depends)
  ['wget -qO- checkip.dyndns.com > my_ip.txt', '/usr/bin/wget']
  >>> t.depends
  [<anadama.deps.StringDependency object at 0x7fd4a6262a50>, <anadama.deps.ExecutableDependency object at 0x7fd4a6262ad0>]

The ``.do()`` method automatically tracks the command string as a
:class:`anadama.deps.StringDependency`. Thus, if the command is
changed within the script, the string dependency will have changed and
the task will not be skipped. This behavior can be deactivated by
setting the keyword option ``track_cmd=False``.

The ``.do()`` method also tries to find and track any scripts, binaries,
or other executables that are used in the command string. Binaries to
track are discovered using
:func:`anadama.runcontext.discover_binaries`. To turn off automatic
binary tracking, set the keyword option ``track_binaries=False``.

For more information on ``.do()``, please refer to the API reference:
:meth:`anadama.runcontext.RunContext.do`.
  

Running the tasks -- ``.go()``
==============================

:meth:`anadama.runcontext.RunContext.go` kicks off the execution of
all the tasks the run context knows about. ``.go()`` can be executed
multiple times within a script; it's perfectly acceptable to add some
tasks, ``.go()``, add more tasks, then ``.go()`` again. All options
pertaining to how the tasks are executed, such as runners and
reporters, can be changed from the ``.go()`` method. Please see
:meth:`anadama.runcontext.RunContext.go` for all available options.

      
