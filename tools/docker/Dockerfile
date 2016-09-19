FROM ubuntu:14.04

############################################################################################
# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
#
# Docker file for building docker image given the kit bundle. Docker file uses
# ubtuntu14.04 as base environment.
#
# Customizable docker properties for build and run time:
#
# Environment variables used at build times that can be passed in using --build-arg at build time
# to package specific version of voltdb kit in docker image
# VOLT_KIT_VERSION  Optional    Specifies the VoltDB kit - kit version suffix of voltdb in
#                               "voltdb-<kit suffix>.tar.gz".
#                               For eg. for voltdb-6.6.tar.gz, suffix will be 6.6.
#                                       for voltdb-6.6rc1.tar.gz, suffix will be 6.6rc1.
#                               If none, provided docker files uses value of is 6.6 which means
#                               it's expecting file called voltdb-6.6.tar.gz file to package
#                               voltdb docker image
# VOLT_DIR_SUFFIX   Optional    Specifies the VoltDB kit directory after extracting the compressed
#                               bundle file - "voltdb-<kit suffix>". For eg, voltdb-6.6.tar gets
#                               extracted to voltdb-6.6 directory, so the directoy suffix is 6.6
#
# Run times customizable behavior using environment variables and mount points. Current docker image
# is designed to run as executable performing "voltdb init" to initialize the cluster node and "voltdb
# start" to start initialized database, by exceuting shell script through entrypoint
#
# Customize behavior for "voltdb init"
# 1 - Deployment file to initialize with: The image gets packaged with default deployment which
#     runs 2 site per host with k-factor of zero. The default deployment can be over-ridden by
#     mounting custom deployment to file /tmp/deployment.xml in container using "-v <custom
#     deployment's absolute path>:/tmp/deployment.xml"
# 2 - Persist data in voltdbroot to host: Mount host location, which will be used as storage for
#     voltdbroot to container path /var/voltdb/ using "-v <host storage location>:/var/voltdb/"
#
# Customize behavior for "voltdb start"
# 1 - Host count needs to be passed in using HOST_COUNT docker environment through
#     "-e HOST_COUNT=<# of nodes>"
# 2 - Hosts list needs to be passed in using HOSTS docker environment through "-e HOSTS=<Comma
#     separated hostnames or IP address>"
# 3 - License file to use: The license can bespecified by mounting license file to file
#     /tmp/license.xml in container using "-v <license file's absolute path>:/tmp/license.xml"
#
############################################################################################

# exit on error
RUN set -e

# Install VoltDB 
# update repo - Trusty does not seem to have add-repo command by default
RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
                                   apt-get upgrade -y && \
                                   apt-get -y  --no-install-recommends --no-install-suggests install python-software-properties && \
                                   apt-get -y  --no-install-recommends --no-install-suggests install software-properties-common

RUN add-apt-repository -y  ppa:openjdk-r/ppa && \
    apt-get update && \
    apt-get install -y  --no-install-recommends --no-install-suggests openjdk-8-jdk wget 


# Set locale-related environment variables
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en

# Set timezone
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone

# Expose the following ports
# Client Port                   21212       
# Admin Port                    21211       
# Web Interface Port (httpd)     8080       
# Internal Server Port           3021       
# Replication Port               5555
# Zookeeper port                 7181
# SSH                              22
EXPOSE 22 3021 5555 7181 8080 8081 9000 21211 21212

ENV VOLTDB_DIST=/opt/voltdb
ENV PATH=$PATH:$VOLTDB_DIST/bin

# set the default version to 6.6
ARG VOLT_KIT_VERSION=6.6
# extract location of voltdb directory's suffix. Decopule it from kit-version as there
# can be case where extract location is  different than voltdb kit version
# for eg. voltdb-6.6rc1 is bundle and extract location can be potentially voltdb-6.6
ARG VOLT_DIR_SUFFIX=${VOLT_KIT_VERSION}

# copy voltdb image and layout it out
COPY voltdb-${VOLT_KIT_VERSION}.tar.gz .
RUN tar -zxf voltdb-${VOLT_KIT_VERSION}.tar.gz && \
    mkdir ${VOLTDB_DIST} && \
    cp -r voltdb-${VOLT_DIR_SUFFIX}/* $VOLTDB_DIST && \
    rm -r voltdb-${VOLT_DIR_SUFFIX} voltdb-${VOLT_KIT_VERSION}.tar.gz

WORKDIR $VOLTDB_DIST

# fetch the deployment file
COPY deployment.xml ${VOLTDB_DIST}
ENV DEFAULT_DEPLOYMENT=$VOLTDB_DIST/deployment.xml
ENV CUSTOM_CONFIG=/tmp/deployment.xml
ENV LICENSE_FILE=/tmp/license.xml

ENV DIRECTORY_SPEC=/var/voltdb/
RUN mkdir $DIRECTORY_SPEC

COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]