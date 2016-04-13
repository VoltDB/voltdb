# Run VoltDB Virtualized, Containerized or in the Cloud

VoltDB has been deployed in production in AWS, Azure, VMWare, Xen, KVM & Docker.

We've run it in test environments in Google Compute, SoftLayer, VirtualBox, Vagrant and more.

Our goal is to make installing VoltDB as simple as installing Java 8 and unpacking the tarball from our website.

Relevant Documentation
-----------------------------------------

The primary getting-started guide is here: 

https://voltdb.com/run-voltdb-virtualized-containerized-or-cloud

There is some additional information on running VoltDB on Docker on our Github wiki page, though it may or may not be 100% up to date.

https://github.com/VoltDB/voltdb/wiki/Docker-&-VoltDB-Clustering-Intro

Guide to Performance and Customization Section 8.3:
Configuring NTP in a Hosted, Virtual, or Cloud Environment
https://docs.voltdb.com/PerfGuide/ntpCloud.php

Additional Notes
-----------------------------------------

VoltDB requires Transparent Huge Pages (THP) to be disabled. When running Virtualized, you may want to turn off THP on the host and guest kernels. When using Docker or other container tech, you need to turn off THP on the host kernel.

https://voltdb.com/blog/linux-transparent-huge-pages-and-voltdb
https://docs.voltdb.com/AdminGuide/adminmemmgt.php