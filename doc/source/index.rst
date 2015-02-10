.. anadama documentation master file, created by
   sphinx-quickstart on Fri Dec 12 15:27:17 2014.

Welcome to AnADAMA's documentation!
===================================

AnADAMA, or Another Automated Data Analysis Management Application is
a tool for performing data analysis in a reproducible, easily scaled,
and extensible way.


Why AnADAMA?
____________

Consider the analyst. She receives data and shall analyze it,
producing some combination of charts, tables, text files, and
documents. Assume the analyst uses scripts to automate the analysis
process. Automated analysis such as this yields many benefits: the
scripts document her work, reduce the time to delivery, and simplify.

If only the analyst's job were so easy. The analyst will need to make
changes to the data products or to the process itself to satisfy
changing job, computational, or other requirements. In the world of
shell scripting, this requires the analyst to battle against
uncertainty: what products must I remake? Do these products depend on
other products or intermediate steps? Furthermore, the analyst may
need to better use computational resources to complete the project on
time; managing multiple processors or multiple machines along with
inter-product dependency is difficult.

AnADAMA surmounts these hurdles. Instead of telling a shell what to do
to complete the analysis, describe the analysis steps to
AnADAMA. AnADAMA will manage dependency structure. Should the list of
steps change, AnADAMA will adapt by only rerunning necessary steps to
produce the desired products. AnADAMA will also manage processing
scale-up and scale-out by running the steps on multiple processors or
multiple machines in the correct order to maintain process
integrity. Should the analyst need to share her process with other
teams or users, AnADAMA provides a ready-made command line interface
to analysis pipelines. Likely, when sharing analysis pipelines, the
pipeline requires resources external to AnADAMA; AnADAMA can track
those resources, too.

* :ref:`getting-started`
* :ref:`scale-up-scale-out`
* :ref:`your-own-pipeline`
* :ref:`example-workflows`


Table of contents
=================

.. toctree::
   :maxdepth: 2

   guides
   api_reference


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

