#!/bin/bash
EXIT=0
if ! ps -ef | grep pup*et; then
  echo "puppet is not running on $HOSTNAME"
  EXIT=1
fi

# what kind of machine does hudson think we are?
LABEL=`ssh newbobbi grep -A7 host.$HOSTNAME .hudson/config.xml | grep label`
if echo $LABEL | grep ubuntu; then OS=Ubuntu; fi
if echo $LABEL | grep CentOS; then OS=el5; fi
if echo $LABEL | grep RedHat; then OS=el6; fi

if ! uname -a | grep $OS; then
  echo "wrong operating system running on $HOSTNAME"
  EXIT="1$EXIT" # there might not be a calculator installed
fi

exit $EXIT
