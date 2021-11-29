This document explains how to run the TopicBenchmark2 workload on a K8S VoltDB cluster.

# System requirements

The test requires an update of the K8S VoltDB cluster so you need the following file that you can find in the pro VoltDB repo, e.g:

    <your_voltdb_pro_workspace>/tests/kubernetes/helm-values/topicbenchmark2-topics.yaml

# Run the default workload

Start the voltdb cluster and wait for it to be ready, e.g.:

    helm install mydb1 voltdb/voltdb --wait --set-file cluster.config.licenseXMLFile=$HOME/voltdb-license.xml \
      --set cluster.clusterSpec.deletePVC=true --set cluster.clusterSpec.persistentVolume.size=20Gi

Once all the pods are ready, upgrade the cluster to declare the topics used by the workload. Note that this requires a helm chart fixing ENG-21745, otherwise upgrade from a local copy of the voltdb-operator.  

Upgrade from the standard helm chart (NOTE: this assumes it has fix to ENG-21745, otherwise you should upgrade using the charts in an up-to-date local repository of the voltdb-operator):

    helm upgrade mydb1 voltdb/voltdb --debug --reuse-values --values=<your_voltdb_pro_workspace>/tests/kubernetes/helm-values/topicbenchmark2-topics.yaml

The pod for the workload can be created using a JSON definition, e.g.:

    kubectl run workloads-topicbenchmark2 --image voltdb/workloads-topicbenchmark2 --overrides='{
      "apiVersion": "v1",
      "spec": {
        "restartPolicy": "Never",
        "imagePullSecrets": [
            { "name": "dockerio-registry" }
        ],
        "containers": [
          {
            "env": [
              {
                "name": "SERVERS",
                "value": "mydb1-voltdb-cluster-0.mydb1-voltdb-cluster-internal.rdykiel.svc.cluster.local "
              }
            ],
            "image": "voltdb/workloads-topicbenchmark2",
            "imagePullPolicy": "Always",
            "name": "workloads-topicbenchmark2",
            "resources": {},
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File"
          }
        ]
      }
    }'

The JSON must define the SERVERS argument pointing to your cluster. It can be further customized to add other arguments to the workload. See the following file in the internal VoltDB repo for the possible arguments:

    <your_voltdb_internal_workspace>/tests/test_apps/topicbenchmark2/Dockerfile

The workload should run until completed:

    kc get pods
    NAME                                    READY   STATUS      RESTARTS   AGE
    mydb1-voltdb-cluster-0                   1/1     Running     0          22m
    mydb1-voltdb-cluster-1                   1/1     Running     0          24m
    mydb1-voltdb-cluster-2                   1/1     Running     0          26m
    mydb1-voltdb-operator-698b56f5cb-xwr4m   1/1     Running     0          27m
    workloads-topicbenchmark2                0/1     Completed   0          46s

The logs of pod 'workloads-topicbenchmark2' can be checked:

    kc logs workloads-topicbenchmark2

The workload pod and the cluster can be deleted:

    kc delete pod workloads-topicbenchmark2
    pod "workloads-topicbenchmark2" deleted

    helm delete mydb1
    release "mydb1" uninstalled

# Build the workload

The workload image is available in Docker at voltdb/workloads-topicbenchmark2:latest.

Should you need to rebuild it, this can be done in the following directory of your VoltDB internal repo, e.g.:

    <your_voltdb_internal_workspace>/tests/test_apps/topicbenchmark2
    ./build_docker.sh

The script updates the 'latest' image in Docker. You may want to use a different tag as follows:

    ./build_docker.sh voltdb/workloads-topicbenchmark2:your_tag

Note that the build script expects a correct full image name.

# Build and run a private workload

You can build the workload and push it to your own Docker repo, by providing the desired image name as an argument to the 'build_docker.sh' script, e.g.:

    ./build_docker.sh rdykiel/topicbenchmark2

The image will be pushed with the 'latest' tag by default. Otherwise you can provide a tag in the argument.  

The workload can be executed in a similar fashion to the default one, by pointing the JSON to your own image.
