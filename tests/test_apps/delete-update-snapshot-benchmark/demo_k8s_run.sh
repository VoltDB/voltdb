#!/bin/bash
#
# Basic demo of the delete-update-snapshot-benchmark running in a k8s container.
#
# To run:
#     gcloud-cluster-create bob
#     pushd ../../../../pro/tests/kubernetes
#     kcc bob apply -f config-kubernetes/gcs-cloudtest-storage/pvc.yaml
#     helmc bob  install db1 voltdb/voltdb \
#       --values helm-values/basic--medium-resources.yaml \
#       --set-file cluster.config.licenseXMLFile=$HOME/voltdb-license.xm
#     popd
#     kwait-ready -c bob -t 240
#     demo_k8s_run.sh --context bob
#
# The worklod will run for 5 or 6 minutes before the server will go into
# pause mode due to 80% of memory being consumed.  It can run longer by
# supply no or larger resources for the DB install, or by tweaking the
# workload parameters (by changing WORKLOAD_PARAMS below).

set -x
kubectl run dusb-workload-db1 --image voltdb/delete-update-snapshot-benchmark "$@" --overrides='{"apiVersion": "v1",
    "spec": {
        "restartPolicy": "Never",
        "containers": [{
            "env": [
                {
                    "name": "WORKLOAD_PARAMS",
                    "value": "--servers=db1-voltdb-cluster-0.db1-voltdb-cluster-internal"
                },
                {
                    "name": "SQLCMD_PARAMS",
                    "value": "--servers=db1-voltdb-cluster-0.db1-voltdb-cluster-internal"
                }
            ],
            "image": "voltdb/delete-update-snapshot-benchmark:latest",
            "imagePullPolicy": "Always",
            "name": "dusb-workload-db1",
            "resources": {},
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
            "volumeMounts": [{
                "name": "cloudtest-storage-bucket",
                "mountPath": "/cloudtest-storage"
            }]
        }],
        "imagePullSecrets": [{
            "name": "dockerio-registry"
        }],
        "volumes": [{
            "name": "cloudtest-storage-bucket",
            "persistentVolumeClaim": {
                "accessMode":"ReadOnlyMany",
                "claimName":"gcs-cloudtest-storage-ro-pvc"
            }

        }]
    }
}'
