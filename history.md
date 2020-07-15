
# AnADAMA2 History #

## v0.7.8 07-15-2020 ##

* Update hclust2 exe check to be python 2 compliant.

## v0.7.7 07-07-2020 ##

* Suppress stdout in hclust2 exe name check.

## v0.7.6 07-07-2020 ##

* Update hclust2 doc method to allow for pip executable.

## v0.7.5 05-28-2020 ##

* Updates to documentation methods for python 3 compatibility.
* Fix issue running without grid scratch set (addition of / to empty string).

## v0.7.4 05-15-2020 ##

* Subclass pweave processor class to modify function to not remove spaces at the end of strings generated from code chunks. This change allows for endlines after markdown images which is needed for pandoc to generate figure captions.
* Update pandoc option to allow for use with latest version.

## v0.7.3 05-04-2020 ##

* Remove extra decode to resolve new error from prior fix for python 3.

## v0.7.2 05-04-2020 ##

* Decode version for tracked executable to resolve reruns for tasks with executable depends in python 3.
* Changes to grid module for python 3.

## v0.7.1 04-07-2020 ##

* For grid scratch, rerun get task result after grid job to get final output files in target output folder resolving issue with function targets not being recorded in database after run success.

## v0.7.0 04-01-2020 ##

* Add grid scratch option for slurm.
* Add config option.

## v0.6.7 01-22-2020 ##

* Update aws batch task script to return error message to grid (in addition to messages already in log).
* Print workflow description and version to log.
* Add to file_size function the ability to get s3 file sizes.

## v0.6.6 01-16-2020 ##

* Fix small variable typo in grid class introduced with AWS grid changes.

## v0.6.5 11-14-2019 ##

* Small fix to allow for logs generated with python2 to be used in reports generated in python3.

## v0.6.4 11-05-2019 ##

* Updates for python 3 compatibility.
* Add paging for AWS batch queue (for large numbers of input files).
* Fix texts so they comply with AWS batch modifications.

## v0.6.3 08-12-2019 ##

* Fix to prevent error with visualization (part 3 of fixes from AWS batch merge: Allow targets/depends to not have temp_files method)

## v0.6.2 08-12-2019 ##

* Fix to prevent error with custom arguments (part 2: only set tmpdir once and fix function calls)

## v0.6.1 08-09-2019 ##

* Fix to prevent error with custom arguments (part 1: set tmpdir after custom args are added)

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

