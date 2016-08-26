FROM ubuntu:14.04
# exit on error
RUN set -e

# Install VoltDB 
# update repo - Trusty does not seem to have add-repo command by default
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get upgrade -y 
RUN DEBIAN_FRONTEND=noninteractive apt-get -y  --no-install-recommends --no-install-suggests install python-software-properties && \
                                   apt-get -y  --no-install-recommends --no-install-suggests install software-properties-common

RUN add-apt-repository -y  ppa:openjdk-r/ppa 

RUN apt-get update && \
    apt-get install -y  --no-install-recommends --no-install-suggests openjdk-8-jdk wget 

# get the voltdb image
COPY voltdb-ent-6.6rc1.tar.gz .
RUN tar -zxvf voltdb-ent-6.6rc1.tar.gz


ENV VOLTDB_DIST=/opt/voltdb

# layout voltdb image and update path
RUN mkdir $VOLTDB_DIST && \
    cp -r voltdb-ent-6.6/* $VOLTDB_DIST && \
    rm -r voltdb-ent-6.6 voltdb-ent-6.6rc1.tar.gz

ENV PATH=$PATH:$VOLTDB_DIST/bin

# Set locale-related environment variables
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en

# Set timezone
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


# Port #                     Default Value         Comments
# Client Port                   21212       
# Admin Port                    21211       
# Web Interface Port (httpd)     8080       
# Internal Server Port           3021       
# Replication Port               5555       VoltDB command line
# Zookeeper port                 7181       VoltDB command line

# Public voltDB ports
EXPOSE 22 3021 5555 8080 8081 9000 21211 21212 7181

WORKDIR $VOLTDB_DIST

# fetch the deployment file
COPY deployment.xml ${VOLTDB_DIST}
ENV DEPLOYMENT=$VOLTDB_DIST/deployment.xml

COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENV DIRECTORY_SPEC=/var/voltdb/
RUN mkdir $DIRECTORY_SPEC

ENTRYPOINT ["./docker-entrypoint.sh"]