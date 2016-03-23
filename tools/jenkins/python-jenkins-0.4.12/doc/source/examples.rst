Using Python-Jenkins
====================

The python-jenkins library allows management of a Jenkins server through
the Jenkins REST endpoints. Below are examples to get you started using
the library.  If you need further help take a look at the :doc:`api`
docs for more details.


Example 1: Get version of Jenkins
---------------------------------

This is an example showing how to connect to a Jenkins instance and
retrieve the Jenkins server version.

::

    import jenkins

    server = jenkins.Jenkins('http://localhost:8080', username='myuser', password='mypassword')
    version = server.get_version()
    print version

The above code prints the version of the Jenkins master running on 'localhost:8080'

From Jenkins vesion 1.426 onward you can specify an API token instead of your
real password while authenticating the user against the Jenkins instance.
Refer to the `Jenkins Authentication`_ wiki for details about how you
can generate an API token. Once you have an API token you can pass the API token
instead of a real password while creating a Jenkins instance.

.. _Jenkins Authentication: https://wiki.jenkins-ci.org/display/JENKINS/Authenticating+scripted+clients


Example 2: Working with Jenkins Jobs
------------------------------------

This is an example showing how to create, configure and delete Jenkins jobs.

::

    server.create_job('empty', jenkins.EMPTY_CONFIG_XML)
    jobs = server.get_jobs()
    print jobs
    server.build_job('empty')
    server.disable_job('empty')
    server.copy_job('empty', 'empty_copy')
    server.enable_job('empty_copy')
    server.reconfig_job('empty_copy', jenkins.RECONFIG_XML)

    server.delete_job('empty')
    server.delete_job('empty_copy')

    # build a parameterized job
    # requires creating and configuring the api-test job to accept 'param1' & 'param2'
    server.build_job('api-test', {'param1': 'test value 1', 'param2': 'test value 2'})
    last_build_number = server.get_job_info('api-test')['lastCompletedBuild']['number']
    build_info = server.get_job_info('api-test', last_build_number)
    print build_info


Example 3: Working with Jenkins Views
-------------------------------------

This is an example showing how to create, configure and delete Jenkins views.

::

    server.create_view('EMPTY', jenkins.EMPTY_VIEW_CONFIG_XML)
    view_config = server.get_view_config('EMPTY')
    views = server.get_views()
    server.delete_view('EMPTY')
    print views


Example 4: Working with Jenkins Plugins
---------------------------------------

This is an example showing how to retrieve Jenkins plugins information.

::

    plugins = server.get_plugins_info()
    print plugins

The above example will print a dictionary containing all the plugins that
are installed on the Jenkins server.  An example of what you can expect
from the :func:`get_plugins_info` method is documented in the :doc:`api`
doc.


Example 5: Working with Jenkins Nodes
-------------------------------------

This is an example showing how to add, configure, enable and delete Jenkins nodes.

::

    server.create_node('slave1')
    nodes = get_nodes()
    print nodes
    node_config = server.get_node_info('slave1')
    print node_config
    server.disable_node('slave1')
    server.enable_node('slave1')

    # create node with parameters
    params = {
        'port': '22',
        'username': 'juser',
        'credentialsId': '10f3a3c8-be35-327e-b60b-a3e5edb0e45f',
        'host': 'my.jenkins.slave1'
    }
    server.create_node(
        'slave1',
        nodeDescription='my test slave',
        remoteFS='/home/juser',
        labels='precise',
        exclusive=True,
        launcher=jenkins.LAUNCHER_SSH,
        launcher_params=params)

Example 6: Working with Jenkins Build Queue
-------------------------------------------

This is an example showing how to retrieve information on the Jenkins queue.

::

    server.build_job('foo')
    queue_info = server.get_queue_info()
    id = queue_info[0].get('id')
    server.cancel_queue(id)


Example 7: Working with Jenkins Cloudbees Folders
-------------------------------------------------

Requires the `Cloudbees Folders Plugin
<https://wiki.jenkins-ci.org/display/JENKINS/CloudBees+Folders+Plugin>`_ for
Jenkins.

This is an example showing how to create, configure and delete Jenkins folders.

::

    server.create_job('folder', jenkins.EMPTY_FOLDER_XML)
    server.create_job('folder/empty', jenkins.EMPTY_FOLDER_XML)
    server.copy_job('folder/empty', 'folder/empty_copy')
    server.delete_job('folder/empty_copy')
    server.delete_job('folder')


Example 8: Updating Next Build Number
-------------------------------------

Requires the `Next Build Number Plugin
<https://wiki.jenkins-ci.org/display/JENKINS/Next+Build+Number+Plugin>`_
for Jenkins.

This is an example showing how to update the next build number for a
Jenkins job.

::

    next_bn = server.get_job_info('job_name')['nextBuildNumber']
    server.set_next_build_number('job_name', next_bn + 50)
