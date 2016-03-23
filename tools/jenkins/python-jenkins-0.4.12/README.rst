README
======

Python Jenkins is a python wrapper for the `Jenkins <http://jenkins-ci.org/>`_
REST API which aims to provide a more conventionally pythonic way of controlling
a Jenkins server.  It provides a higher-level API containing a number of
convenience functions.

We like to use python-jenkins to automate our Jenkins servers. Here are some of
the things you can use it for:

* Create new jobs
* Copy existing jobs
* Delete jobs
* Update jobs
* Get a job's build information
* Get Jenkins master version information
* Get Jenkins plugin information
* Start a build on a job
* Create nodes
* Enable/Disable nodes
* Get information on nodes
* Create/delete/reconfig views
* Put server in shutdown mode (quiet down)
* List running builds
* Create/delete/update folders [#f1]_
* Set the next build number [#f2]_
* Install plugins
* and many more..

To install::

    $ sudo python setup.py install

Online documentation:

* http://python-jenkins.readthedocs.org/en/latest/

Developers
----------
Bug report:

* https://bugs.launchpad.net/python-jenkins

Repository:

* https://git.openstack.org/cgit/openstack/python-jenkins

Cloning:

* git clone https://git.openstack.org/openstack/python-jenkins

Patches are submitted via Gerrit at:

* https://review.openstack.org/

Please do not submit GitHub pull requests, they will be automatically closed.

More details on how you can contribute is available on our wiki at:

* http://docs.openstack.org/infra/manual/developers.html

Writing a patch
---------------

We ask that all code submissions be flake8_ clean.  The
easiest way to do that is to run tox_ before submitting code for
review in Gerrit.  It will run ``flake8`` in the same
manner as the automated test suite that will run on proposed
patchsets.

Installing without setup.py
---------------------------

Then install the required python packages using pip_::

    $ sudo pip install python-jenkins

.. _flake8: https://pypi.python.org/pypi/flake8
.. _tox: https://testrun.org/tox
.. _pip: https://pypi.python.org/pypi/pip


.. rubric:: Footnotes

.. [#f1] The free `Cloudbees Folders Plugin
    <https://wiki.jenkins-ci.org/display/JENKINS/CloudBees+Folders+Plugin>`_
    provides support for a subset of the full folders functionality. For the
    complete capabilities you will need the paid for version of the plugin.

.. [#f2] The `Next Build Number Plugin
   <https://wiki.jenkins-ci.org/display/JENKINS/Next+Build+Number+Plugin>`_
   provides support for setting the next build number.
