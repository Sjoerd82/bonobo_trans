.. bonobo-trans documentation master file, created by
   sphinx-quickstart on Fri Feb  8 18:09:07 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: logo.png
   :align: center
   :alt: logo
   :height: 128px
   :width: 128px

**bonobo-trans** provides a set of ETL transformations for the `Bonobo ETL toolkit`_.

.. _Bonobo ETL toolkit: http://www.bonobo-project.org  

.. toctree
   :maxdepth: 2
   
   transformations
  
Transformations
---------------

- :doc:`source/source`
- :doc:`source/target`
- :doc:`source/lookup`
- :doc:`source/sequencer`
- :doc:`source/sorter`
- :doc:`source/aggregator`

Requirements
------------

- bonobo 0.6.3
- pandas
- sqlalchemy

Installation
------------

Install bonobo-trans by running::

    $ pip install bonobo-trans

Contribute
----------
   
- We need a code review!
- ReadTheDocs TOC is missing structure (and I seem to be unable to figure this out).


Support
-------

If you are having issues, please let me know.
I can be reached via Slack's Direct Messaging on bonobo-etl.slack.com

License
-------

The project is licensed under the Apache license.
