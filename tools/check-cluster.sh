#!/bin/bash

for i in `echo 3a 3b 3c 3d 3e 3f 3g 3h 3i 3j 3k 3l 4a 4b 4c`; do
    echo $i
    ssh volt$i ps -ef | grep java | grep -v slave.jar && exit
done

