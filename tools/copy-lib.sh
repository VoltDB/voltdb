#!/bin/bash

# ENG-8472: Add both Mac and Linux native libraries to the voltdb-<rel>.jar.
#  That way, we can release a single kit, whether tarball, .deb, or .rpm that
#  contains all the binaries to run on either platform
#
# For automated builds only; harmless in a dev build but adds no value
# so enabled only in the "test" user context

if [ "$USER" != "test" ]; then
    exit 0
fi

echo '$1 is' $1
echo '$2 is' $2
echo 'lib is' obj/release/nativelibs/libvoltdb*


OS=`uname`

if [ "$OS" = "Darwin" -a -e obj/release/nativelibs/libvoltdb*.jnilib ]
then
    # the Mac case...
    tar cf - obj/release/nativelibs/ | ssh volt0 "cd libs; tar xf -"
    exit 0
else
    # the Linux case...
    if [ -e ~/libs/obj/release/nativelibs/* ]; then
        mkdir -p $1
        cp ~/libs/obj/release/nativelibs/* $1
        exit 0
    else
        echo "++++++++++++++nativelibs not found!"
        exit 0
    fi
fi
