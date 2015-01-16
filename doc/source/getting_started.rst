.. _getting-started:

############################
Getting started with AnADAMA
############################


AnADAMA is essentially DoIt_ with a few extensions:

- Extra command to serialize the DoIt action plan into a JSON document
- All DoIt tasks are serialized into runnable python scripts.
- AnADAMA defines a :ref:`pipeline`, which chains doit tasks for
  larger jobs
- Track external dependencies like executables and binaries

.. _DoIt: http://pydoit.org/


Installation
============

Just a one-liner::

  pip install -e  'git+https://bitbucket.org/biobakery/anadama.git@master#egg=anadama-0.0.1'


A bit of terminology
====================

Workflow
    A python function that returns a single `doit task dict
    <http://pydoit.org/tasks.html>`_.

Pipeline
    A python class that subclasses :py:class:`pipelines.Pipeline` and
    contains multiple workflows chained together to produce many types
    of products from many types of inputs.

