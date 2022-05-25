set -x
kubectl run kvbenchmark-workload-db1 --image voltdb/kvbenchmark "$@" --overrides='{"apiVersion": "v1",
    "spec": {
        "restartPolicy": "Never",
        "containers": [{
            "env": [
                {
                    "name": "SERVERS",
                    "value": "db1-voltdb-cluster-0.db1-voltdb-cluster-internal"
                },
                {
                    "name": "DURATION",
                    "value": "7200"
                }
            ],
            "image": "voltdb/kvbenchmark:latest",
            "imagePullPolicy": "Always",
            "name": "kvbenchmark-workload-db1",
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
