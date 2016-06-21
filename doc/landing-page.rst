AnADAMA
#######

*Another Automated Data Analysis Management Application*

.. contents::

________________________________________________________________________________

Using Version 2
===============

To start working with the new features of AnADAMA, use the ``v2`` branch.

::

  $ git clone https://bitbucket.org/biobakery/anadama.git
  $ cd anadama
  $ git checkout v2
  $ python setup.py install


Documentation for version 2 can be found at the `docs site <https://huttenhower.sph.harvard.edu/docs/anadamav2/guides.html>`_.




Overview
========

AnADAMA is essentially doit_ with a few extensions:

- Extra command to serialize the DoIt action plan into a JSON document
- All DoIt tasks are serialized into runnable python scripts.
- Executes tasks on compute clusters LSF and SLURM without having to
  directly interact with the queueing system.
- Defines pipelines, collections of doit tasks, and provides interfaces to pipelines.

.. _doit: http://pydoit.org/

Installation
============

One liner::

  $ pip install -e 'git+https://bitbucket.org/biobakery/anadama.git@master#egg=anadama-0.0.1'


Usage
=====

Looking to use AnADAMA for microbiome sequence analysis?
Check out the ``anadama_workflows`` repository_ over on bitbucket.

Otherwise,

Basic usage is covered in a presentation_ over on bitbucket.

.. _presentation: http://rschwager-hsph.bitbucket.org/2014-07-11_lab-presentation/index.html#/3 


Documentation
=============

Head on over to the docs_ site.

.. _docs: http://huttenhower.sph.harvard.edu/docs/anadama/index.html

How to get help
===============

Submit an issue_.

.. _issue: https://bitbucket.org/biobakery/anadama/issues

Workflows and pipelines written for you
=======================================

Hey, it's nice to use things that are already made!

Check out the ``anadama_workflows`` repository_ over on bitbucket.

.. _repository: https://bitbucket.org/biobakery/anadama_workflows