#!/bin/bash

for i in `echo 3a 3b 3c 3d 3e 4a 4b 4c`; do
    echo $i
    ssh volt$i ps -ef | grep java && exit
done

