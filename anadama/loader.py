import re
import importlib
import inspect
from collections import defaultdict

from doit.cmd_base import TaskLoader
from doit.exceptions import InvalidCommand

from .util import filespec, matcher
from .pipelines import Pipeline
        
opt_pipeline_argument = { 
    "name"    : "pipeline_arg",
    "long"    : "pipeline_arg",
    "short"   : "f",
    "type"    : list,
    "default" : [],
    "help"    : ("Arguments (typically, files; hence -f) "
                 "to provide to the pipeline in 'key: value' format.\n"
                 "Check the pipeline's documentation for appropriate options.\n"
                 "Specify multiple options with multiple -f 'opt' flags.\n"
                 "Values beginning with 'glob:' are expanded by shell glob.\n"
                 "Values beginning with 're:' are expanded by regex.\n"
                 "Non-glob and non-regex values with commas are interpreted "
                 "as a list.")
}

opt_append_pipeline = {
    "name"    : "append_pipeline",
    "long"    : "append_pipeline",
    "short"   : "A",
    "type"    : list,
    "default" : [],
    "help"    : ("Append this pipeline to the main pipeline.\n"
                 "Append multiple with many -A flags")
}

opt_pipeline_option = {
    "name"    : "pipeline_option",
    "long"    : "pipeline_option",
    "short"   : "o",
    "type"    : list,
    "default" : [],
    "help"    : ("Options to give to the workflows composing this pipeline.\n"
                 "Specify options in 'workflowname.key: value' format "
                 "e.g. sequence_convert.lenfilters_list: >=100.\n"
                 "For boolean options, leave the value blank e.g. "
                 "humann2.pick_frames: \n"
                 "For lists, use commas e.g. "
                 "sequence_convert.lenfilters_list: >=100,<300.\n"
                 "For nested dictionaries, use more periods e.g. "
                 "demultiplex.qiime_opts.M: 2")
}
                
opt_skiptasks = {    
    "name"    : "skip_tasks",
    "long"    : "skip_tasks",
    "short"   : "k",
    "type"    : list,
    "default" : [],
    "help"    : (
        "defines criterion by which AnADAMA will filter out or skip when \n"
        "executing tasks in 'key: value' format. Add multiple filters with \n"
        "multiple -k flags. Children skipped tasks will also be skipped\n"
        "Example: 'name: humann' will skip any tasks that contain 'humann' \n"
        "in the task 'name' attribute and any tasks that depend on those \n"
        "tasks."
)}

opt_data_directory = {    
    "name"    : "data_directory",
    "long"    : "data_dir",
    "short"   : "d",
    "type"    : str,
    "default" : './',
    "help"    : "Base directory to search for files to feed into the pipeline."
}

opt_products_directory = {
    "name"    : "products_directory",
    "long"    : "products_dir",
    "type"    : str,
    "default" : './anadama_products',
    "help"    : "Base directory to save data products."
}


RE_COLON = re.compile(r':\s*')


class PipelineLoader(TaskLoader):

    cmd_options = (opt_pipeline_argument, opt_data_directory, 
                   opt_pipeline_option, opt_products_directory, 
                   opt_append_pipeline, opt_skiptasks)

    def __init__(self, *args, **kwargs):
        self._pipeline_cls = None
        super(PipelineLoader, self).__init__(*args, **kwargs)


    @property
    def pipeline_cls(self):
        return self._pipeline_cls


    @pipeline_cls.setter
    def pipeline_cls(self, value):
        if isinstance(value, Pipeline):
            self._pipeline_cls = value
        elif type(value) is str:
            self._pipeline_cls = self._import(value)
        else:
            raise TypeError("Acceptable values are module "
                            "strings or anadama.Pipeline")


    def load_tasks(self, cmd, opt_values, pos_args):
        args, kwargs = self._parse_args(self.pipeline_cls, opt_values, pos_args)
        pipeline = self._init_pipeline(self.pipeline_cls, args, kwargs)
        for pipeline_name in opt_values['append_pipeline']:
            cls = self._import(pipeline_name)
            args, kwargs = self._parse_args(cls, opt_values, pos_args)
            optional_pipeline = self._init_pipeline(cls, args, kwargs)
            pipeline.append(optional_pipeline)

        try:
            config = pipeline.configure()
        except TypeError as e:
            raise InvalidCommand("Pipeline improperly configured: "+e.message)

        return pipeline.tasks(), config


    def _parse_args(self, pipe_cls, opt_values, pos_args):
        self.data_dir = opt_values['data_directory']

        keyword_arguments = dict()
        keyword_arguments['products_dir'] = opt_values['products_directory']
        file_options = self._parse_file_arguments(opt_values, pipe_cls)
        keyword_arguments.update(file_options)

        wf_options = self._parse_workflow_options(opt_values, pipe_cls)
        keyword_arguments['workflow_options'] = wf_options

        skiptasks = self._parse_skiptasks(opt_values['skip_tasks'])
        keyword_arguments['skipfilters'] = skiptasks

        return [], keyword_arguments


    def _parse_file_arguments(self, opt_values, pipe_cls):
        for opt in opt_values['pipeline_arg']:
            key, val = self._pipeline_option_split(opt)
            if key in pipe_cls.products:
                files = self._parse_file_pattern(val, self.data_dir)
                yield key, files
            else:
                msg = "Invalid argument: `%s'. Possibly you meant: `%s'"%(
                    key, matcher.find_match(key, pipe_cls.products.keys()))
                raise InvalidCommand(msg)


    def _parse_workflow_options(self, opt_values, pipe_cls):
        ret = defaultdict(dict)
        for workflow_opt_str in opt_values['pipeline_option']:
            workflow_name, key_value = self._workflow_option_split(
                workflow_opt_str)
            if workflow_name in pipe_cls.default_options:
                self._workflow_option_update(ret[workflow_name], key_value)
            else:
                match = matcher.find_match(workflow_name, 
                                           pipe_cls.default_options.keys())
                raise InvalidCommand(
                    "Invalid option key: `%s'. Possibly you meant: `%s'"%(
                        workflow_name, match)
                )


        return dict(ret)
        

    @staticmethod
    def _parse_skiptasks(filter_strs):
        ret = []
        for s in filter_strs:
            try:
                key, regex = re.split(RE_COLON, s, 1)
            except:
                raise InvalidCommand(
                    "Unable to parse skip_task pattern."
                    "Be sure to split keys and values with ': '.")
            try:
                regex = re.compile(regex)
            except Exception as e:
                raise InvalidCommand(
                    "Unable to parse regular expression: "+e.message)

            the_filter = lambda task_dict: regex.search(task_dict[key])
            ret.append(the_filter)

        return ret


    @staticmethod
    def _workflow_option_split(opt_str):
        try:
            name_key, value = re.split(RE_COLON, opt_str, 1)
            if ',' in value: 
                value = value.split(",")
        except ValueError:
            value = None

        packed = name_key.split('.')
        if len(packed) < 2:
            raise InvalidCommand(
                ("Unable to determine workflow name in option %s. "
                 "Remember to prefix workflow options with the "
                 "workflow name e.g. humann2.pick_frames")%(opt_str))
        elif len(packed) == 2:
            name, key = packed
            key_value = (key, value)
        else:
            name = packed[0]
            key_value = packed[1:] + [value]
                
        return name, key_value


    @staticmethod
    def _parse_file_pattern(pattern_str, data_dir):
        try:
            return filespec.parse(pattern_str, data_dir=data_dir)
        except OSError as e:
            raise InvalidCommand("Unable to expand %s: %s"%(pattern_str, e))


    @staticmethod
    def _pipeline_option_split(option):
        try:
            key, val = re.split(RE_COLON, option, 1)
        except ValueError:
            raise InvalidCommand(
                ("Unable to parse pipeline option %s."
                 "Keys must be separated with ': ' from values.")%(option)
            )
        return key, val


    @staticmethod
    def _workflow_option_update(option_dict, key_value):
        lead, key, value = key_value[:-2], key_value[-2], key_value[-1]
        prev = option_dict
        last = option_dict
        for level in lead:
            nesteddict = prev.get(level)
            if nesteddict:
                last = prev[level]
                prev = last
            else:
                last = prev[level] = dict()
                
        last[key] = value
        return


    @staticmethod
    def _import(pipeline_name):
        try:
            mod, pipeline_name = re.split(RE_COLON, pipeline_name, 1)
            module = importlib.import_module(mod)
            return getattr(module, pipeline_name)
        except (ImportError, AttributeError) as e:
            raise InvalidCommand(e.message)
        except ValueError:
            raise InvalidCommand(
                "Unable to understand module name. "
                "Try something like 'anadama_workflows.pipelines:WGSPipeline'"
            )


    @staticmethod
    def _init_pipeline(cls, args, kwargs):
        try:
            return cls(*args, **kwargs)
        except TypeError as e:
            spec = inspect.getargspec(cls.__init__)
            required_args = spec.args[1:-len(spec.defaults)]
            raise InvalidCommand("%s. Required arguments: %s"
                                 %(str(e), required_args))

