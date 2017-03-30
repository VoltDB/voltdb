#!/bin/bash



OUTPUT="/tmp/wweiss-whileif.log"



status=0

echo > $OUTPUT

while [ $status == 0 ]; do

    $1

    status=$?

    echo $status >> $OUTPUT

done
