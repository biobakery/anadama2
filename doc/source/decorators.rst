decorators
##########


.. contents:: 
   :local:
.. currentmodule:: anadama.decorators

.. automodule:: anadama.decorators
   :members: 
   :undoc-members:

   .. py:decorator:: requires(binaries=list(), version_methods=list())
      Track the dependencies of a workflow function. 
      
      When the wrapped function is called, the decorator searches for
      the named binaries/executables in the directories named in the
      shell's PATH variable. If the binary/executable is not found,
      the decorator raises a ``ValueError``.

      The workflow function must return a single ``task`` dictionary.

      As a side effect of importing modules with functions wrapped in
      this decorator, the strings in the ``binaries`` and
      ``version_methods`` variables are added to
      :py:data:`provenance.BINARY_PROVENANCE`.


      :keyword binaries: List of the names of installed
			 executables. Executables should be on the
			 shell's PATH variable
      :type binaries: List of strings.

      :keyword version_methods: List of commands to run to determine
				the executables version. The commands
				in this list should correspond to the
				executables in the ``binaries``
				parameter.
      :type version_methods: List of strings.

