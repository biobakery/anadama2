.. _getting-started:

############################
Getting started with AnADAMA
############################


AnADAMA is essentially DoIt_ with a few extensions:

- Extra command to serialize the DoIt action plan into a JSON document
- All DoIt tasks are serialized into runnable python scripts.
- AnADAMA defines a :class:`~anadama.pipelines.Pipeline`, which chains
  doit tasks for larger jobs
- Track external dependencies like executables and binaries


Installation
============

Just a one-liner::

  pip install -e  'git+https://bitbucket.org/biobakery/anadama.git@master#egg=anadama-0.0.1'



How to use AnADAMA
==================

Remember, AnADAMA is DoIt_ at its core. Thus, one of the ways to use
AnADAMA is by creating your own `dodo.py file
<http://pydoit.org/tasks.html#intro>`_ and running ``anadama run``.

Another way to run AnADAMA is to use the command line interface to an
existing pipeline with ``anadama pipeline``. Some pipelines already
exist in :py:mod:`anadama_workflows.pipelines`. See
:py:mod:`anadama.pipelines` and the output of ``anadama help``.

AnADAMA can also serialize its action plan to a JSON-formatted
document. See :py:class:`anadama.commands.ListDag`,
:py:class:`anadama.commands.DagPipeline`, and the output of 
``anadama help dag``.


.. _DoIt: http://pydoit.org/



A bit of terminology
====================

Workflow
    A python function that returns a single `doit task dict
    <http://pydoit.org/tasks.html>`_.

Pipeline
    A python class that subclasses :class:`anadama.pipelines.Pipeline` and
    contains multiple workflows chained together to produce many types
    of products from many types of inputs.


