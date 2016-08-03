AnADAMA
#######

*Another Automated Data Analysis Management Application*

.. contents::

________________________________________________________________________________

Overview
========

AnADAMA is a python module to track dependency changes and rerun
workflow tasks only when necessary. Plus, you get these extra goodies for free:

- A pretty progress indicator to show run status
- Executes tasks on compute clusters LSF and SLURM without having to
  directly interact with the queueing system.
- A framework for building pipelines, for when you need an interface
  to your workflows fast.

Getting AnADAMA
===============

Downloading the code
____________________

.. code:: bash

  $ hg clone https://bitbucket.org/biobakery/anadama2
  $ cd anadama2

Installation
____________

.. code:: bash

  $ python setup.py install


Running the tests
_________________

.. code:: bash

  $ python setup.py test	  


Usage
=====

.. note:: Looking to use AnADAMA for microbiome sequence analysis?
   Check out the ``anadama_workflows`` repository_ over on bitbucket.



Documentation
=============

Head on over to the docs_ site.

.. _docs: http://huttenhower.sph.harvard.edu/docs/anadamav2/index.html

How to get help
===============

Submit an issue_.

.. _issue: https://bitbucket.org/biobakery/anadama2/issues

