AnADAMA
#######

*Another Automated Data Analysis Management Application*

.. contents::

_________________________________________________________________________________


Overview
========

AnADAMA is essentially DoIt_ with a few extensions:

- Extra command to serialize the DoIt action plan into a JSON document
- All DoIt tasks are serialized into runnable python scripts.
- Defines pipelines, collections of doit tasks, and provides interfaces to pipelines.

.. _DoIt: http://pydoit.org/

Installation
============

One liner::

  $ pip install -e 'git+https://bitbucket.org/biobakery/anadama.git@master#egg=anadama-0.0.1'


Usage
=====

Basic usage is covered in a presentation_ over on bitbucket.

.. _presentation: http://rschwager-hsph.bitbucket.org/2014-07-11_lab-presentation/index.html#/3 


Documentation
=============

Head on over to the docs_ site.

.. _docs: http://rschwager-hsph.bitbucket.org/documentation/anadama/index.html

How to get help
===============

Submit an issue_.

.. _issue: https://bitbucket.org/biobakery/anadama/issues

Workflows and pipelines written for you
=======================================

Hey, it's nice to use things that are already made!

Check out the ``anadama_workflows`` repository_ over on bitbucket.

.. _repository: https://bitbucket.org/biobakery/anadama_workflows
