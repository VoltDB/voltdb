FROM ubuntu:14.04
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
ARG RELEASE_VERSION=6.6

# copy voltdb image and layout it out
COPY voltdb-${RELEASE_VERSION}.tar.gz .
RUN tar -zxvf voltdb-${RELEASE_VERSION}.tar.gz && \
    mkdir $VOLTDB_DIST && \
    cp -r voltdb-${RELEASE_VERSION}/* $VOLTDB_DIST && \
    rm -r voltdb-${RELEASE_VERSION} voltdb-${RELEASE_VERSION}.tar.gz

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