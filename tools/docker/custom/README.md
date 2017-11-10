Docker/Custom
---------------


Docker/Custom is a set of helper scripts for creating custom VoltDB Docker images. It 
allows images targeting specific Volt versions and OS distributions. This is useful for
testing against a Docker image other than the community Volt image.

Running ./build.sh and specifying the target OS distribution produces a runnable Volt
image. It builds VoltDB from the currently checked out source, so to produce a build of
say, 7.7, the 7.7 branch must be checked out.

For example, assuming the 7.7 branch of Volt has been checked out, the 
following will produce a runnable Docker image for Volt 7.7 on Ubuntu 16.04 
(Here a proxy is also specified).

```
OS_DIST=ubuntu-16.04 PROXY=10.0.0.3:3142 ./src/build.sh
```

A helper script to start a node cluster is also provided. For example the following 
will start a Volt cluster using the Docker image for Volt 7.7 on Ubuntu 16.04. (If a
Volt version is not supplied it will default to using the version in the locally checked 
out version.txt file).

```
VOLTDB_VERSION=7.7 OS_DIST=ubuntu-16.04 ./src/run.sh
```

At present ubuntu 14.04 and ubuntu 16.04 Dockerfiles have been supplied.
