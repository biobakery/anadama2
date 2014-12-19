import re
from itertools import chain

from doit.task import dict_to_task
from doit.loader import flat_generator
from doit.control import no_none

from . import dag

def Matcher(str_or_callable):
    if type(str_or_callable) is str:
        return lambda file_: bool(re.search(str_or_callable, file_) is not None)
    if hasattr(str_or_callable, '__call__'):
        return lambda file_: bool(str_or_callable(file_))
    else:
        raise TypeError("Matcher accepts only string or callable,"
                        "received %s"%(type(str_or_callable)))


def route(files, rules):
    """Fit a list of files into a dictionary of strings -> lists of
    strings by the rules defined in ``rules``. Returns a dictionary that
    can be used as keyword args for other functions (likely
    pipelines), where the keys are the name of the keyword argument,
    and the values are lists of strings. `rules` should be an iterable
    of tuples.

    ``rules``'s keys (the first item in the tuple) can be either
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
    Subclass this to make your own pipelines.
    The interface for a pipeline in a task is as follows::

        def task_use_my_pipeline():
            my_pipeline = SomePipeline(raw_files=['groceries.txt', 
                                                  'bucket_list.txt'])
            my_pipeline.configure()
            yield my_pipeline.tasks()

    For use in a ``loader``, use the task_dicts attribute like so::

        from doit.cmd_base import Loader
        class MyLoader(Loader):
            def load_tasks(self, cmd, opt_values, pos_args):
                my_pipeline = SomePipeline(raw_files=['groceries.txt', 
                                                      'bucket_list.txt'])
                global_config = my_pipeline.configure()
                return my_pipeline.tasks(), global_config

    """

    name = None
    products = dict()
    
    def __init__(self, skipfilters=None):
        """Instantiate the Pipeline. Doesn't do too much in the base
        class. Subclass this to put your own options and pipeline
        inputs.

        Here's an example::

            from andama.pipelines import Pipeline
            class SomePipeline(Pipeline):
                def __init__(self, the_inputs=list(), *args, **kwargs):
                    # don't forget to do the initialization steps from 
                    # the superclass by using super()
                    super(SomePipeline, self).__init__(*args, **kwargs)
                    self.inputs = the_inputs

        """
        self.task_dicts = None
        self.skipfilters = skipfilters
        self.products = self.products.copy()

        if not self.name:
            self.name = self.__class__.__name__
            
    def __iter__(self):
        if self.task_dicts:
            return iter(self.task_dicts)
        else:
            return iter([])

    def _configure(self):
        """Configures a pipeline, yielding doit task_dicts as a
        generator. This should be where the bulk of your pipeline
        goes.

        """
        raise NotImplementedError()


    def add_products(self, **kwargs):
        self.products.update(kwargs)
        for name, value in kwargs.iteritems():
            setattr(self, name, value)


    def configure(self):
        """Configure the workflows associated with this pipeline by calling
        the _configure function.

        Return the global doit config dict.

        Side effect: populate the task_dicts attribute with a list of
        doit tasks. You'll need this to get the tasks() method to
        return something other than an empty generator.

        """
        default_tasks = list()
        self.task_dicts = list()
        self._configure = no_none(self._configure)
        nested_dicts = self._configure()
        flat_dicts = iter( d for d, _ in flat_generator(nested_dicts) )
        for d in self.filter_tasks(flat_dicts):
            default_tasks.append(d["name"])
            self.task_dicts.append( d )

        # return the global doit config dictionary
        return {
            "default_tasks": default_tasks,
            "continue":      True,
            "pipeline_name": self.name
        }

    __call__ = configure


    def tasks(self):
        """Call this method to get tasks (not task dicts) from a pipeline."""
        for d in self.task_dicts:
            yield dict_to_task(d)


    def filter_tasks(self, task_dicts):
        if self.skipfilters:
            return dag.filter_tree(task_dicts, self.skipfilters)
        else:
            return task_dicts
            
    @classmethod
    def _chain(cls, other_pipeline, workflow_options=dict()):
        needed_products = cls.products.keys()
        product_attributes = dict([
            (attr, getattr(other_pipeline, attr))
            for attr in needed_products
            if hasattr(other_pipeline, attr)
        ])
        if not product_attributes:
            raise ValueError(
                "Cannot chain to pipeline %s: missing at least one of %s"%(
                    other_pipeline.name, needed_products))
        else:
            return cls(workflow_options=workflow_options,
                       products_dir=other_pipeline.products_dir,
                       **product_attributes)

    
    def append(self, other_pipeline_cls):
        other_pipeline = other_pipeline_cls._chain(self)
        self._old_configure = self._configure
        def _new_configure():
            first = self._old_configure()
            second = other_pipeline._configure()
            return chain(first, second)

        self._configure = _new_configure
        self.name += ", "+other_pipeline.name
        self.products.update(
            (key, value) 
            for key, value in other_pipeline.products.iteritems() 
            if key not in self.products
        )

        return self
