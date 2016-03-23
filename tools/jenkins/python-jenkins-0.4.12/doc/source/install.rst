:title: Installing

Installing
==========

The module is known to pip and Debian-based distributions as
``python-jenkins``.

``pip``::

    pip install python-jenkins

``easy_install``::

    easy_install python-jenkins

The module has been packaged since Ubuntu Oneiric (11.10)::

    apt-get install python-jenkins

And on Fedora 19 and later::

    yum install python-jenkins

For development::

    python setup.py develop


Documentation
-------------

Documentation is included in the ``doc`` folder. To generate docs
locally execute the command::

    tox -e docs

The generated documentation is then available under
``doc/build/html/index.html``.

Unit Tests
----------

Unit tests have been included and are in the ``tests`` folder.  We recently
started including unit tests as examples in our documentation so to keep the
examples up to date it is very important that we include unit tests for
every module.  To run the unit tests, execute the command::

    tox -e py27

* Note: View ``tox.ini`` to run tests on other versions of Python.

Due to how the tests are split up into a dedicated class per API method, it is
possible to execute tests against a single API at a time. To execute the tests
for the :py:meth:`.Jenkins.get_version` API execute the command::

    tox -e py27 -- tests.test_version.JenkinsVersionTest

For further details on how to list tests available and different ways to
execute them, see https://wiki.openstack.org/wiki/Testr.

Test Coverage
-------------

To measure test coverage, execute the command::

    tox -e cover
