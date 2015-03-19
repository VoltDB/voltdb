#!/usr/bin/env bash

die() {
    echo "FATAL: $@"
    exit 1
}

find_root() {(
    while [ ! -d .git ]; do
        cd ..
        test "$(pwd)" == "/" && break
    done
    test -d .git && pwd
)}

V_ROOT=$(find_root)
test -z "$V_ROOT" && die "Not in a VoltDB development workspace."

V_VERSION=$(cat "$V_ROOT/version.txt")

check_jar() {
    local PATH="$V_ROOT/obj/$1/dist/voltdb/voltdb-$V_VERSION.jar"
    test -f "$PATH" && echo "$PATH"
}

V_RELEASE_JAR="$(check_jar release)"
V_DEBUG_JAR="$(check_jar debug)"

test -z "$V_RELEASE_JAR" -a -z "$V_DEBUG_JAR" && die "No distribution build found."
if [ -n "$V_RELEASE_JAR" -a -n "$V_DEBUG_JAR" ]; then
    if [ "$V_DEBUG_JAR" -nt "$V_DEBUG_JAR" ]; then
        V_JAR=$V_DEBUG_JAR
    else
        V_JAR=$V_RELEASE_JAR
    fi
else
    if [ -n "$V_DEBUG_JAR" ]; then
        V_JAR=$V_DEBUG_JAR
    else
        V_JAR=$V_RELEASE_JAR
    fi
fi

V_DIST=$(dirname $(dirname $V_JAR))

CMD_build() {
    echo "workspace: $V_ROOT"
    echo "version: $V_VERSION"
    echo "distribution: $V_DIST"

    cd "$V_DIST"

    echo "\
    FROM debian:jessie
    MAINTAINER VoltDB <info@voltdb.com>

    # External ports
    EXPOSE 8080
    EXPOSE 8081
    EXPOSE 9000
    EXPOSE 21211
    EXPOSE 21212

    # Internal ports
    EXPOSE 3021
    EXPOSE 4560
    EXPOSE 9090

    ENV VOLTDB_DIST /opt/voltdb
    ENV PATH \$PATH:\$VOLTDB_DIST/bin
    ENV VOLTDB_HEAPMAX 1024

    ADD bin \$VOLTDB_DIST/bin/
    ADD lib \$VOLTDB_DIST/lib/
    ADD version.txt \$VOLTDB_DIST/
    ADD voltdb \$VOLTDB_DIST/voltdb/

    RUN apt-get update
    RUN apt-get install -qy --no-install-recommends procps psmisc python openjdk-7-jre-headless

    WORKDIR /opt/voltdb
    #CMD [\"python\", \"web.py\", \"8081\"]
    " > Dockerfile

    echo "\
<?xml version="1.0"?>
<deployment>
    <cluster hostcount="1" kfactor="0" />
    <httpd enabled="true">
        <jsonapi enabled="true" />
    </httpd>
</deployment>
" > deployment.xml

    docker build --force-rm=true "$@" .
}

_port() {
    local PORT=$(docker ps | awk "match(\$0,/0\.0\.0\.0:([0-9]+)->$1\//,g) {print g[1]}")
    test -z "$PORT" && die "Port $1 not found."
    echo $PORT
}

CMD_sql() {
    local PORT=$(_port 21212)
    sqlcmd --port=$PORT
}

CMD_clientport() {
    _port 21212
}

if [ -z "$1" ]; then
    echo "Usage: docker.sh build|clientport|sql"
    exit 1
fi

"CMD_$@"
