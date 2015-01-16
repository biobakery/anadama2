provenance
##########


.. contents:: 
   :local:
.. currentmodule:: anadama.provenance

.. automodule:: anadama.provenance
   :members:
   :undoc-members:

   .. py:data:: BINARY_PROVENANCE

      A set of 2-tuples, where each 2-tuple is composed of two
      strings: the name of a executable available on the shell's PATH,
      and a command which will give the version of that executable.

      This module-level variable is populated upon import of functions
      wrapped with :py:obj:`decorators.requires`.

