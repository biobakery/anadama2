from doit.task import dict_to_task
from doit.loader import flat_generator
from doit.control import no_none

class Pipeline(object):
    """ Class that encapsulates a set of workflows.
    """
    
    def __init__(self):
        """ Instantiate the Pipeline
        """
        self.tasks   = None


    def _configure(self):
        """Configures a pipeline, yielding tasks as a generator. Should
        generally be overridden in base classes

        """
        raise NotImplementedError()


    def configure(self):
        """Configure the workflows associated with this pipeline by calling
        the _configure function.

        Return the global doit config dict.

        Side effect: populate the tasks attribute with a list of doit tasks

        """
        default_tasks = list()
        self.tasks = list()
        self._configure = no_none(self._configure)
        task_dicts = self._configure()
        for d, _ in flat_generator(task_dicts):
            default_tasks.append(d["name"])
            self.tasks.append( dict_to_task(d) )

        # return the global doit config dictionary
        return {
            "default_tasks": default_tasks,
            "continue":      True
        }


    def tasks(self):
        for task in self.tasks:
            yield task
