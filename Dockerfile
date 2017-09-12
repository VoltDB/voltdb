FROM ubuntu:16.10
# exit on error
RUN set -e
ADD . /voltdb

RUN apt-get update
RUN apt-get install -y ant build-essential ant-optional default-jdk
RUN apt-get install -y python cmake
RUN apt-get install -y valgrind 
RUN apt-get install -y ntp
RUN apt-get install -y ccache
RUN apt-get install -y python-httplib2
RUN apt-get install -y python-setuptools
RUN apt-get install -y python-dev

# build voltdb
WORKDIR /voltdb
RUN ant clean dist

#ENV VOLTDB_BUNDLE = voltdb-ent-6.6rc1.tar.gz
RUN cp obj/release/voltdb-7.7.tar.gz .
RUN tar xvf voltdb-7.7.tar.gz
#COPY {VOLTDB_BUNDLE} .
#RUN tar -zxvf ${VOLTDB_BUNDLE}


ENV VOLTDB_DIST=/opt/voltdb

# layout voltdb image and update path
RUN mkdir $VOLTDB_DIST && \
    cp -r voltdb-7.7/* $VOLTDB_DIST && \
    rm -r voltdb-7.7 voltdb-7.7.tar.gz

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

#RUN echo "set entry point"
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh
ENTRYPOINT ["./docker-entrypoint.sh"]


ENV DIRECTORY_SPEC=/var/voltdb/
RUN mkdir $DIRECTORY_SPEC

#RUN bin/voltdb init -C deployment.xml -D ${DIRECTORY_SPEC}

#CMD ["sh", "-c", "$VOLTDB_DIST/bin/voltdb start --ignore=thp -D ${DIRECTORY_SPEC} -c ${HOST_COUNT} -H ${HOSTS}"]
#CMD ["sh", "-c", "$VOLTDB_DIST/bin/voltdb start --ignore=thp -D ${DIRECTORY_SPEC} "]
#CMD ["bin/voltdb start"]
