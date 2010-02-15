#!/bin/bash

for i in `echo 3a 3b 3c 3d 3e 3f 4a 4b 4c`; do
    echo $i
    ssh volt$i pkill java
done

