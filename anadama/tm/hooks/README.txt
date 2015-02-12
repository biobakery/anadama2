This area is for user generated code which will be executed at the
time given by the name of the script file:

pre_pipeline.sh - runs prior to the start of the pipeline
post_pipeline_success.sh - runs after the successful completion of the pipeline
post_pipeline_failure.sh - runs after the failure to complete the pipeline
post_task_failure.sh - runs after each task failure
post_task_success.sh - runs after each task success

Tasks that have already completed successfully via a previous
run are skipped and do not spawn a post_task_success.sh process.
Likewise, tasks that are marked as failed either because of a 
signal interrupt at the command line or because a dependant task
failed do not spawn a post_task_failure.sh process.

The scripts themselves are bourne shell but obviously anything can
be spawned from them.  The shell environment is preset with the
following name value pairs:

TaskName
TaskResult
TaskReturnCode
TaskScriptfile
TaskLogfile
TaskProducts
