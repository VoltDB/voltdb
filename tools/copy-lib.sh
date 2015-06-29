#!/bin/bash

echo $1 | ssh volt3f "cat > libs/arg"
if [ -e "obj/release/nativelibs/libvoltdb-5.4.jnilib" ]; then
    tar cf - obj/release/nativelibs/ | ssh volt3e "cd libs; tar xf -"
fi
