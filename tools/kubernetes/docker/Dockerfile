#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# VoltDB pristine container
# This container is intended for k8s stateful set deployments

FROM ubuntu:16.04
MAINTAINER Phil Rosgay <prosegay@voltdb.com>

ARG IP_DIR
ARG VOLTDB_DIST_NAME
ARG NODECOUNT

# Public VoltDB ports
EXPOSE 22 5555 8080 8081 9000 21211 21212

# Internal VoltDB ports
EXPOSE 3021 4560 9090 5555

# Set up environment
RUN apt update
RUN apt-get update
RUN apt-get -y --no-install-recommends --no-install-suggests install sudo software-properties-common python-software-properties
RUN apt-get -y --no-install-recommends --no-install-suggests install alien
RUN add-apt-repository ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN apt-get -y --no-install-recommends --no-install-suggests install oracle-java8-installer
RUN apt-get -y --no-install-recommends --no-install-suggests install libxml2-utils procps python vim less curl jq
RUN apt-get -y --no-install-recommends --no-install-suggests install dnsutils net-tools iputils-ping telnet iperf
RUN apt-get -y --no-install-recommends --no-install-suggests install python-pip python-setuptools python-dev build-essential
RUN pip install --upgrade pip
RUN pip install --upgrade httplib2
RUN locale-gen en_US.UTF-8

# Set VoltDB environment variables
ENV VOLTDB_DIST=/opt/$VOLTDB_DIST_NAME
ENV PATH=$PATH:$VOLTDB_DIST/bin

# Set locale-related environment variables
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

# Set timezone
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Create necessary directories, voltdb-kit, voltdb_pristine

# For voltdb bundles and extensions, copy your assets to the corresponding directories in the kit
# prior to building the image.

WORKDIR /opt

RUN mkdir -p $VOLTDB_DIST
COPY ./ $VOLTDB_DIST/
RUN rm -rf $VOLTDB_DIST/doc $VOLTDB_DIST/examples
COPY tools/kubernetes/bin/ $VOLTDB_DIST/bin
