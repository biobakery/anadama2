
# AnADAMA2 History #

## v0.6.1 08-09-2019 ##

* Fix to prevent error with custom arguments

## v0.6.0 07-01-2019 ##

* Added grid option for AWS batch with input/output files written to S3

## v0.5.3 03-14-2019 ##

* Changes to colorbar plots to handle NAs

## v0.5.2 01-11-2019 ##

* Add colorbar for continuous data in pcoa plots
* Update max labels on plots as we can fit a few more sample names
* Increase space in heatmap for metadata labels

## v0.5.1 11-01-2018 ##

* Allow for optional data files in the archiving of a document.

## v0.5.0 08-21-2018 ##

* Add distance function keyword to show_hclust2 doc method
* Add method to plot multiple poca colored by abundance
* In document show_pcoa, set default to not apply arcsin-sqrt transform and add caption for this case
* Detect new rc exceeded memory limit reported in error file to slurm class
* Make cli argument keys a list for python3 compatibility
* In document formatter, allow for older versions of pandoc by only using width in figure if newer version of pandoc is installed

## v0.4.0 10-19-2017 ##

* Add read of variables from log to LoggerReporter class method read_log.
* Add metadata keyword to hclust2 method.
* Add new method plot_stacked_barchart_grouped.
* Update pweave section to use custom formatter class (to fix pweave issue of duplicating plots in single code block).
* Add metadata option to pcoa plot.
* Include any document dependencies in the output data folder so they can be bundled with the report archive.
* Update sge default partition to match new broad configuration.

## v0.3.1 07-24-2017 ##

* The API is now hosted at Readthedocs. The html has been removed from the source since it is now automatically generated.
* Bitbucket pipelines have been setup for automatic testing for python2 and python3 installs.
* A new option was added to allow users to provide commands to set up the environment of the grid tasks.
* Jupyter notebook examples have been added.
* For builds based on targets, relative paths are now allowed to targets.
* The dpi and font size for the document heatmaps were modified to improve rendering.
* Unique names are allowed for tasks in groups.
* Instead of raising errors, document heatmaps and grouped barcharts print errors to the document to make debugging easier. 

## v0.3.0 06-02-2017 ##

* AnADAMA2 is now pip installable. 

