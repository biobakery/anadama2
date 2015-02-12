from doit.cmd_base import Command

from ..provenance import find_versions


class BinaryProvenance(Command):
    name = "binary_provenance"
    doc_purpose = "print versions for required dependencies"
    doc_usage = "<module> [<module> [<module...]]"
    
    def execute(self, opt_values, pos_args):
        """Import workflow modules as specified from positional arguments and
        determine versions of installed executables and other
        dependencies via py:ref:`provenance.find_versions`.

        For each external dependency installed, print the name of the
        dependency and the version of the dependency.

        """

        for mod_name in pos_args:
            for binary, version in find_versions(mod_name):
                print binary, "\t", version

