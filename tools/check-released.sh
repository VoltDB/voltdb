#!/bin/bash

# check whether someone is using this machine without having reserved it first

if ps -ef | grep java | grep -v "^test"; then
  java -jar ~/.hudson/hudson-cli.jar -s http://newbobbi:8080 offline-node $HOSTNAME -m "hudson found machine not cleaned up after manual tests"
  exit 1
fi

exit 0
