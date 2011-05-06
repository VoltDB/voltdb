#!/bin/bash
# check that puppet is running and start it if it's not
hostname
EXIT=0
#if ! ps -ef | grep pup*et; then
#  echo "puppet is not running on $HOSTNAME -- restarting"
#  # try to restart
#  /usr/bin/ruby /usr/sbin/puppetd --server=volt1 || EXIT=1
#  if ! ps -ef | grep pup*et; then
#    echo "puppet is still not running on $HOSTNAME"
#    EXIT=1
#  fi
#fi

cd /tmp && ls -tr | grep volt_snapshot | head -n-10 | xargs rm -f

exit $EXIT
