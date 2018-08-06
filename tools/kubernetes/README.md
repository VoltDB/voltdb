[comment]: # (This file is part of VoltDB.)
[comment]: # (Copyright © 2008-2018 VoltDB Inc.)

# Running a VoltDB Database Using Kubernetes

The accompanying scripts provide support for running VoltDB in a Kubernetes environment.
Use these scripts as templates, customizing them as described in the following sections
to match your requirements and/or environment.

## Setting Up the Deployment

Here's the procedure to setup a k8s deployment of VoltDB:

1. Download and untar the voltdb release kit.

2. cd kubernetes folder (this folder)

3. Make a new CONFIG_FILE by copying config_template.sh, then customize it as follows:

    1. Set REP to the docker repository to push the image to
    2. Set IMAGE_TAG to the image tag
    3. Set NODECOUNT to the number of voltdb nodes
    4. Set LICENSE_FILE to the location of your voltdb license file
    5. Set DEPLOYMENT_FILE to the location of your VoltDB deployment file
    6. Set SCHEMA_FILE to the location of your database startup schema (optional)
    7. Set CLASSES_JAR to the location of your database startup classes (options)
    8. Set EXTENSION_DIR, BUNDLES_DIR, LOG4J_CUSTOM_FILE  to the location of your lib-extension, bundes, or logging properties (optional)

4. Build the image and voltdb-statefulset deployment file:

    This step creates the voltdb image and configures a kubernetes statefullset deployment
    for a VoltDB cluster. The name of CONFIG_FILE is used as the cluster name. 
    with provided assets, and a initial database root is created with your settings.
    This root will be copied to persistent storage on first run of the database (node).

        ./build_image.sh CONFIG_FILE

    input: CONFIG_FILE.cfg
           VoltDB release kit  
    output: container (docker) image pushed to a designated docker repository
            voltdb-statefulset.yaml configured and renamed

    Example: ./build_image.sh voltdb-boston.cfg

    A voltdb-boston.yaml k8s stateufset deployment file which loads a container image named 'voltdb-boston'.
    The deployed cluster will be discoverable in k8s as (<pod>.<service>.default.svc.cluster.local):
        voltdb-boston-<n>.voltdb-boston.default.svc.cluster.local.

## Starting and Stopping the Cluster

* To start the cluster:

        kubectl create -f K8S_DEPLOYMENT

* To stop the cluster, retaining the persistent volume(s):

        voltadmin pause --wait
        kubectl delete -f K8S_DEPLOYMENT

* To display persistent volume(s) and claim(s):

        kubectl get pvc
        kubectl get pv

    
* To delete volumes (all data in the database will be lost):

        kubectl delete <pv/pvc name>

    nb. next time the database is brought up it will be in the initial state

## Other Useful Commands

Here are some other commands that are useful (assuming the name of the statefulset is "voltdb" and its pods are voltdb-0, ...)

* To connect sqlcmd to a running cluster

        kubectl exec -it voltdb-0 -- sqlcmd --servers=localhost

* To proxy VoltDB ports to localhost ports (ports are remote[:local])

        kubectl port-forward statefulset/<name> 8080:8080 21212:21212 21211

   You can then run voltdb commands locally, for example:

        $VOLTDB_HOME/bin/sqlcmd --servers=localhost --port=21212
        $VOLTDB_HOME/bin/voltadmin ...
