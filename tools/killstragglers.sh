#!/bin/bash

# kill org.voltdb processes that aren't BenchmarkController

# list all process pids and the name of the associated executable
# look for java executables
# use ps to see the command arguments for those java pids
# find args that look like an org.voltdb class main entry point
# that aren't benchmark controller
# and kill them.

# as one line:
# ps cx -o pid -o command  | grep java | awk '{print $1}' | xargs ps -w -w -o pid -o args -p | \
#  grep " org.voltdb" | grep -v BenchmarkController | awk '{print $1}'  | xargs kill -9

# sudo kill if USER is 'test' and we're in the evening test hours on specific volt hosts...
# if you need to run overnight, please choose a machine which is not hit by this. thanks.

HOUR=`date +%H`
if [ $USER = "test" ] && ( [ $HOUR -ge 22 ] || [ $HOUR -le 7 ] ); then
   for H in volt{3{a,b,c,d,e,f},7{b,c,d},5{b,c,d},12{b,c,d,e,f}}
   do
      if [ $HOSTNAME = $H ]; then
        SUDO=sudo
        break
      fi
   done
fi
Z=`$SUDO jps -l | grep " org.voltdb" | grep -v BenchmarkController | awk '{print $1}' | xargs`
[ -n "$Z" ] && $SUDO kill -9 $Z
exit 0
