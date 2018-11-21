[comment]: # (This file is part of VoltDB.)
[comment]: # (Copyright © 2008-2018 VoltDB Inc.)

# Running a VoltDB Database Using Kubernetes

The accompanying scripts provide support for running VoltDB in a Kubernetes environment.
Use these scripts as templates, customizing them as described in the following sections
to match your requirements and/or environment.

## Setting Up the Deployment

Here's the procedure to setup a k8s deployment of VoltDB:

1. Download and untar the voltdb release kit.

2. cd kubernetes folder (this folder). In this folder you will find:

    voltdb-k8s-utils.sh             Script to deploy voltdb clusters
    config_template.cfg             Deployment Parameter template (requires customization)
    voltdb-statefulset.yaml         k8s deployment spec (template)


3. The script voltdb-k8s-utils.sh along with an accompanying parameter file will enable you to deploy a VoltDB Cluster
   in k8s. The script has the following functions:

    voltdb-k8s-utils.sh <cluster-parameter-file> <option>

    <cluster-parameter-file> has the key-value pairs for the cluster settings
    <options> one or more of the following options.

    See the description of the individual functions for details

    -B, --build-voltdb-image        Builds the VoltDB docker image using docker build

    -M, --install-configmap         Installs the configmap for VoltDB init using kubectl create configmap

    -C, --configure-voltdb          Generates a statefulset.yaml to deploy the VoltDB cluster using
                                    voltdb-statefulset.yaml as a master template

    -S, --start-voltdb              Deploys the VoltDB cluster and starts the nodes using kubectl scale/create

    -P, --stop-voltdb               Gracefully shutdown VoltDB using kubectl exec voltadmin shutdown

    -D, --purge-persistent-claims   Purge persistent volume (PERSISTENT DATA WILL BE LOST)
                                    using kubectl delete pv ...

    -F, --force-voltdb-set          Terminate VoltDB statefulset ungracefully (POSSIBLE DATA LOSS)
                                    using kubectl delete pods and statefulsets for the cluster

    If you want to more than one option in an invocation, specify them separately and in the order
    in which you want them to execute. ie. "-B -M -C -S"



4. Make a new CONFIG_FILE by copying config_template.cfg, name this file with your new cluster's name.

    The filename that you choose for your template will be used as the Cluster Name for the voltdb cluster,
    all outputs will be identified by this name.

    1. Set REP to the docker repository to push the image to (optional)
    2. Set IMAGE_TAG to the image tag (optional)
    3. Set NODECOUNT to the number of voltdb nodes (required)
    4. Set MEMORYSIZE to the required memory size to request from k8s for each node (default 4Gi)
    5. Set CPU_COUNT to the required number of cpus to request from k8s for each node (default 1)
    6. Set PVOLUME_SIZE to the size of the persistent volume needed for each node (default 1Gi)
    7. Set LICENSE_FILE to the location of your voltdb license file (optional, default from the voltdb kit)
    8. Set DEPLOYMENT_FILE to the location of your VoltDB deployment file (optional)
    9. Set SCHEMA_FILE to the location of your database startup schema (optional)
   10. Set CLASSES_JAR to the location of your database startup classes (options)
   11. Set EXTENSION_DIR, BUNDLES_DIR, LOG4J_FILE  to the location of your lib-extension, bundles, or logging properties (optional)
   12. Set VOLTDB_START_ARGS additional command line arguments for voltdb start (optional)
   13. Set VOLTDB_OPTS runtime options for voltdb start (optional)
   14. Set VOLTDB_HEAPMAX runtime java heap allocation for voltdb start (optional)

    Notes:

    Passing startup parameters to VoltDB:

    You can pass parameters to voltdb at startup by setting VOLTDB_START_ARGS in the k8s deployment yaml:

        VOLTDB_START_ARGS="--ignore=thp --pause ..."

    See VoltDB documentation for more information about startup options.

    For your deployment there may be other things to configure such as persistent volume properties, sizes, etc.,
    you'll need to edit the voltdb-statefulset.yaml template and set your specific requirements there, and check
    it into your source control.

    For these voltdb start options you may specify either a single value or a comma separated list of values.
    If a list is given, a single value corresponding to the pod ordinal will be selected from the list
    at voltdb startup.

                '--externalinterface'
                '--internalinterface'
                '--publicinterface'
                '--admin'
                '--client'
                '--http'
                '--internal'
                '--replication'
                '--zookeeper'
                '--drpublic'

                ex: --drpublic '10.0.0.1,10.0.0.2,10.0.0.3,10.0.0.4,...'
                for statefulset pod-1 --drpublic 10.0.0.2 will be used at startup


5. Build the image config-maps, and customized voltdb-statefulset deployment file:

        ./voltdb-k8s-utils.sh CONFIG_FILE -B -M -C

        chained comands:
        -B: build the image
        -M: configure and install the config-maps (both init-time and run-time)
        -C: configure the customized statefulset yaml

6. Start the deployment:

        ./voltdb-k8s-utils.sh CONFIG_FILE -S


## Starting and Stopping the Cluster

* To stop the cluster:

        ./voltdb-k8s-utils.sh CONFIG_FILE -P  (or -F to force it off without saving)

* To restart the cluster:

        ./voltdb-k8s-utils.sh CONFIG_FILE -R

        the database will be quiesced, followed by a scale down to zero pods.

* To purge the persistent volumes the cluster:

        !!! This will result in unercoverable loss of the database and its data !!!

        ./voltdb-k8s-utils.sh CONFIG_FILE -D

* To display persistent volume(s) and claim(s):

        kubectl get pvc
        kubectl get pv


## Other Useful Commands

Here are some other commands that are useful (assuming the name of the statefulset is "voltdb" and its pods are voltdb-0, ...)

* To connect sqlcmd to a running cluster

        kubectl exec -it pod-0 -- sqlcmd --servers=localhost

* To proxy VoltDB ports to localhost ports (ports are remote[:local])

        kubectl port-forward statefulset/<cluster name> 8080 21212 21211

   You can then run voltdb commands locally, for example:

        $VOLTDB_HOME/bin/sqlcmd --servers=localhost --port=21212
        $VOLTDB_HOME/bin/voltadmin ...
