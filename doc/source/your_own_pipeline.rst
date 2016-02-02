.. _your-own-pipeline:

############################################
Using Existing Pipelines and making your own
############################################

Some pipelines already written. These pipelines may be useful to you
outright, or they may serve as an example for writing your own
pipelines. Some existing pipelines are in the ``anadama_workflows``
module

  * :py:class:`anadama_workflows.pipelines.wgs.WGSPipeline`
  * :py:class:`anadama_workflows.pipelines.sixteen.SixteenSPipeline`


.. _directory-skeleton-howto:

Using pipelines via the directory skeleton
==========================================

Use the ``skeleton`` command. This command creates a skeleton of input
directories for a pipeline and a working dodo.py file. Place your
input files into the appropriate directories, then run ``anadama run``
or ``doit run``. Workflow options can be changed by editing the files
under the ``input/_options`` directory.

The option files are written in YAML_. Here's an example of specifying
an option like ``-o 'pick_otus_closed_ref.qiime_opts.jobs_to_start:
6'``::

  # Edit a file named input/_options/pick_otus_closed_ref.txt
  
  qiime_opts:
      jobs_to_start: 6


An example of a true/false option like
``-o 'pick_otus_closed_ref.qiime_opts.a: '``::

  #Edit a file named input/_options/pick_otus_closed_ref.txt

  qiime_opts:
      a: true


Let's put both options together::

  #Edit a file named input/_options/pick_otus_closed_ref.txt

  qiime_opts:
      jobs_to_start: 6
      a: true
  

Here's an example usage::

  $ anadama skeleton anadama_workflows.pipelines:WGSPipeline

  Constructing input skeleton at /tmp/input.
  Creating input directory /tmp/input/sample_metadata for <type 'list'>...Done.
  Creating input directory /tmp/input/intermediate_fastq_files for <type 'list'>...Done.
  Creating input directory /tmp/input/raw_seq_files for <type 'list'>...Done.
  Creating input directory /tmp/input/metaphlan_results for <type 'list'>...Done.
  Creating input directory /tmp/input/otu_tables for <type 'list'>...Done.
  Creating input directory /tmp/input/decontaminated_fastq_files for <type 'list'>...Done.
  Writing default options for anadama_workflows.pipelines:WGSPipeline.infer_pairs into /tmp/input/_options/infer_pairs.txt...Done.
  Writing dodo.py file...Done.
  Writing help file to README.rst...Done.
  Complete.

  $ anadama
  ...

In the course of your work you may find it necessary to pass in a
collection of settings for your ipelines as needed.  You can use the
following procedure::

  1) Execute "anadama skeleton WGS" to create the hierarchy of directories.
  2) Under the top-level directory, edit "input/_options/decontaminate.txt"
     to include the location(s) of the database(s), as indicated below:
     
     reference-db:
     - /path/to/my/db1
     - /path/to/my/db2


.. _yaml: http://yaml.org/spec/1.1/#id857168

Using pipelines via the command line interface
==============================================


Getting help
____________

Help is always good. Use 
``anadama help pipeline <module:pipelineName>`` to print all
documentation available on a given pipeline. The pipeline name is
represented in a dotted module syntax, with the pipeline class' name
behind a colon. Submodules of any depth can be used with the
appropriate amount of dots. For more information on dotted module
syntax see `modules <https://docs.python.org/2/tutorial/modules.html>`_.

Use ``anadama pipeline`` to run a pipeline via the command
line. ``anadama help pipeline`` provides documentation on the specific
flags and general usage mechanics. 


Pipeline arguments
__________________

Pipelines take arguments and options. Arguments, typically file names
for input into various parts of the pipeline, are specified with the
``-f`` flag. Multiple arguments are specified with multiple ``-f``
flags. Arguments are specified in a key-value format, where the key
and the value are separated by a colon. Argument keys must be named
according to the names expected by the pipeline. To find the argument
names expected by a pipeline, use ``anadama help pipeline
<module:pipelineName>``. Some argument values have special
meaning. The value is interpreted as a list, with each file separated
by a comma. Values can also be specified using shell-style globbing
patterns: a value of ``glob:*.fastq`` will be interpreted as all FASTQ
files in current directory and be passed to the pipeline as a list of
file names. In a similar fashion, values be given as a regular
expression: ``re:SRS[0-9]+.sff`` would pass all numerical sff files
beginning with "SRS" in the current directory to the pipeline.  Some
example key-value arguments:

  * A one item list: ``-f 'sample_metadata: map.txt'``
  * Multiple item list: ``-f 'raw_seq_files: a.fastq,b.fastq,c.fastq'``
  * A shell glob: ``-f 'demuxed_fasta_files: glob:*.fna'``


Pipeline Options
________________

Pipelines are composed of many workflows. Typically, each workflow has
options. Each option is specified on the command line in a key-value
format with the ``-o`` flag. Multiple options are specified with
successive ``-o`` flags. Similar to the pipeline name, these options
are specified in a dotted format. Keys and values are separated by a
colon. The first part of the key is the name of a workflow. The second
and further parts of the key are the name of the option and any nested
dictionaries of options. Names are separated by dots. Values can be
either a list or a string; including a comma on the value splits the
value into a list. Some examples of options:

  * Vanilla option: ``-o 'metaphlan2.ignore_markers: not_these.txt'``
  * Nested option: ``-o 'pick_otus_closed_ref.qiime_opts.jobs_to_start: 6'``
  * Nested, boolean option: ``-o 'pick_otus_closed_ref.qiime_opts.a: '``


Skipping parts of the pipeline
______________________________

Skip certain tasks in the pipeline with the ``-k`` flag. Multiple
rules are specified with multiple ``-k`` flags. Each rule should be a
key-value pair separated by a colon ``:``. The rule defines criterion
by which AnADAMA will filter out or skip when executing tasks. The key
in each key-value pair is the task field on which the filter operates,
while the value (the string that comes after the colon) is a regular
expression. If the regular expression matches contents of the field
named in the key, the task is skipped. **All children of the skipped
task will be also be skipped.** Here's an example: ``-k name:humann``
will skip any tasks that contain ``humann`` in the task's ``name``
attribute.


Appending additional pipelines
______________________________

Some pipeline classes can be appended to other pipeline to add more
functionality. An example of this is the
:py:class:`anadama_workflows.pipelines.vis.VisualizationPipeline`. Appendable
pipelines can be used on their own, or can be stuck onto the end of
another pipeline to pick up where the first pipeline left off. To
append such a pipeline on the command line, use the ``-A``
flag. Specify the pipeline using the same dotted module syntax used
to specify the main pipeline. Here's an example::

  -A anadama_workflows.pipelines:VisualizationPipeline


Putting it all together
_______________________

Below are some examples that tie in all the above information. 

Pick OTUs with the ``anadama_workflows`` 16S pipeline using an already
demultiplexed set of sequences and passing the ``-a`` and
``--jobs_to_start=6`` to qiime's ``pick_closed_reference_otus.py``::

  anadama pipeline anadama_workflows.pipelines:SixteenSPipeline \
      -f 'sample_metadata: map.txt' \
      -f 'demuxed_fasta_files: seqs.fasta' \
      -o 'pick_otus_closed_ref.qiime_opts.a: ' \
      -o 'pick_otus_closed_ref.qiime_opts.jobs_to_start: 6'


Perform the default human DNA scrubbing and taxonomic profiling from
the ``anadama_workflows`` WGS pipeline, but skip the humann2 steps::

  anadama pipeline anadama_workflows.pipelines:WGSPipeline \
      -f 'raw_seq_files: glob:*.bam' \
      -f 'sample_metadata: map.txt' \
      -k 'name: humann2'



Using pipelines in a DoIt or AnADAMA environment
================================================

The interface for a pipeline in a task is as follows::

  def task_use_my_pipeline():
      my_pipeline = SomePipeline(raw_files=['groceries.txt', 
					    'bucket_list.txt'])
      my_pipeline.configure()
      yield my_pipeline.tasks()




##########################################
Sharing your process - Creating a pipeline
##########################################

Subclass :py:class:`anadama.pipelines.Pipeline`.
