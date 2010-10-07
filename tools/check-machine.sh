#!/bin/bash
# check that puppet is running and start it if it's not
EXIT=0
if ! ps -ef | grep pup*et; then
  echo "puppet is not running on $HOSTNAME -- restarting"
  # try to restart
  /usr/bin/ruby /usr/sbin/puppetd --server=volt1 || EXIT=1
  if ! ps -ef | grep pup*et; then
    echo "puppet is still not running on $HOSTNAME"
    EXIT=1
  fi
fi

# check whether hudson has the right idea what kind of machine we are
LABEL=`ssh newbobbi grep -A7 host.$HOSTNAME .hudson/config.xml | grep label`
if echo $LABEL | grep ubuntu; then OS=Ubuntu; fi
if echo $LABEL | grep CentOS; then OS=el5; fi
if echo $LABEL | grep RedHat; then OS=el6; fi
if echo $LABEL | grep fedora; then OS=fc10; fi

if ! uname -a | grep $OS; then
  echo "wrong operating system running on $HOSTNAME"
  EXIT="1$EXIT" # there might not be a calculator installed
fi

exit $EXIT
