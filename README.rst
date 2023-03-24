faculty-datasets-fs
===================

An fsspec plugin for Faculty datasets.

Installation
------------

.. code-block:: sh

    pip install faculty-datasets-fs

Usage
-----

``faculty-datasets-fs`` adds a ``faculty-datasets`` protocol that can be used
with ``fsspec``, for example:

.. code-block:: python

    with fsspec.open("faculty-datasets://file.txt") as fp:
        fp.read()

``faculty-datasets-fs`` relies on environment variables and the standard
Faculty credentials toolchain to determine:

* The Faculty deployment to query
* The Faculty project to query
* Credentials to use

These will work out of the box when running within a Faculty project.
