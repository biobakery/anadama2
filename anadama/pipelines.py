import re

from doit.task import dict_to_task
from doit.loader import flat_generator
from doit.control import no_none

def Matcher(str_or_callable):
    if type(str_or_callable) is str:
        return lambda file_: bool(re.search(str_or_callable, file_) is not None)
    if hasattr(str_or_callable, '__call__'):
        return lambda file_: bool(str_or_callable is True)
    else:
        raise TypeError("Matcher accepts only string or callable,"
                        "received %s"%(type(str_or_callable)))


def route(files, rules):
    """Fit a list of files into a dictionary of strings -> lists of
    strings by the rules defined in `rules`. Returns a dictionary that
    can be used as keyword args for other functions (likely
    pipelines), where the keys are the name of the keyword argument,
    and the values are lists of strings. `rules` should be an iterable
    of tuples.

    `rules`'s keys (the first item in the tuple) can be either
    functions or strings. Strings are interpreted as regular
    expressions; files are fit into the list with the first regex
    match.  Functions are called on the string and put into the first
    list which resulted in a True from the function. Order matters!

    Example::

        ret = route(['Joe', 'Bob', 'larry'],
                    [ (r'o',                          "names_with_o")
                      (lambda val: val.endswith('y'), "endswith_y") ]
                    )
        ret
        { 'names_with_o': ['Joe', 'Bob'], 'endswith_y': ['larry'] }

    """

    ret =      dict([ (value, list())         for _, value in rules ])
    matchers = dict([ (Matcher(key), value) for key, value in rules ])
    for file_ in files:
        for matcher, name in matchers.iteritems():
            if matcher(file_):
                ret[name].append(file_)
    
    return ret
                

class Pipeline(object):
    """ Class that encapsulates a set of workflows.
    """
    
    def __init__(self):
        """ Instantiate the Pipeline
        """
        self.task_dicts = None


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
        self.task_dicts = list()
        self._configure = no_none(self._configure)
        nested_dicts = self._configure()
        for d, _ in flat_generator(nested_dicts):
            default_tasks.append(d["name"])
            self.task_dicts.append( d )

        # return the global doit config dictionary
        return {
            "default_tasks": default_tasks,
            "continue":      True
        }


    def tasks(self):
        for d in self.task_dicts:
            yield dict_to_task(d)
