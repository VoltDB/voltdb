#!/bin/bash

OS=`uname`
echo $1 | ssh volt3f "cat > libs/arg"
if [ "$OS" = "Darwin" -a -e "obj/release/nativelibs/libvoltdb-5.4.jnilib" ]
then
    tar cf - obj/release/nativelibs/ | ssh volt3e "cd libs; tar xf -"
else
   mkdir -p $1
   cp ~/libs/obj/release/nativelibs/* $1
fi
