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

    1. Set the docker repository to push the image to
    2. Set the image tag
    3. Set the location of your voltdb license file
    4. Set the location of your voltdb deployment file
    5. Set the location of your database startup schema (optional)
    6. Set the location of your database startup classes (options)
    7. Set the location of your lib-extension, bundes, or logging properties (optional)

4. Build the image and voltdb-statefulset deployment file:

    This step creates the voltdb image and configures a kubernetes deployment file.
    The resulting deployment file will be named... The container image is customized
    with provided assets, and a initial database root is created with your settings.
    This root will be copied to persistent storage on first run of the database (node).

    `./build_image.sh CONFIG_FILE`

## Starting and Stopping the Cluster

* To start the cluster:

  `kubectl create -f K8S_DEPLOYMENT`

* To stop the cluster, retaining the persistent volume(s):

    ```
    voltadmin pause --wait
    kubectl delete -f K8S_DEPLOYMENT
    ```
* To display persistent volume(s) and claim(s):

    ```
    kubectl get pvc
    kubectl get pv
    ```
    
* To delete volumes (all data in the database will be lost):

    `kubectl delete <pv/pvc name>`

    nb. next time the database is brought up it will be in the initial state

## Other Useful Commands

Here are some other commands that are useful (assuming the name of the statefulset is "voltdb" and its pods are voltdb-0, ...)

* To connect sqlcmd to a running cluster

   `kubectl exec -it voltdb-0 -- sqlcmd --servers=localhost`

* To proxy VoltDB ports to localhost ports (ports are remote[:local])

   `kubectl port-forward statefulset/<name> 8080:8080 21212:21212 21211`

   You can then run voltdb commands locally, for example:

   `$VOLTDB_HOME/bin/sqlcmd --servers=localhost --port=21212`

   `$VOLTDB_HOME/bin/voltadmin ...`
