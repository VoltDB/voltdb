#!/bin/bash

# Kill any/all java processes that are listening on any port(s)
# sudo kill if USER is 'test' and we're in the evening test hours and/or on specific volt hosts...
# If you need to run overnight, schedule with qa.

HOUR=`date +%H`
DAY=`date +%u`

if [ $USER = "test" ]; then
    case `hostname` in
        volt10?|volt12?|volt13?)
            SUDO=sudo
            ;;
        *)
            if [ $DAY -ge 6 ] || [ $HOUR -ge 20 ] || [ $HOUR -le 9 ]; then
                SUDO=sudo
            fi
            ;;
    esac
fi
for P in `$SUDO netstat -tnlp | egrep 'LISTEN.*/java' | tr -s \  | cut -d\  -f7 | cut -d\/ -f1 | sort | uniq`
do
    logger -isp user.notice -t TESTKILL "$BUILD_TAG Killing `$SUDO ps --no-headers -p $P -o user,command`"
    $SUDO kill -9 $P
done
exit 0
