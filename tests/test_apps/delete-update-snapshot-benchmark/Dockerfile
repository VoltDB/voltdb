FROM openjdk:8-jre-alpine

# bash is required by sqlcmd
RUN apk add --no-cache bash

# any and all arguments to pass along to the workload program
ARG WORKLOAD_PARAMS
# any and all arguments to pass along to sqlcmd if it's needed
ARG SQLCMD_PARAMS

COPY . /delete-update-snapshot-benchmark/

WORKDIR /delete-update-snapshot-benchmark

CMD ./docker_run.sh
