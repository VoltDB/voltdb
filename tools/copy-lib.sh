#!/bin/bash

# ENG-8472: Add both Mac and Linux native libraries to the voltdb-<rel>.jar.
#  That way, we can release a single kit, whether tarball, .deb, or .rpm that
#  contains all the binaries to run on either platform
#
# For automated builds only; harmless in a dev build but adds no value
# so enabled only in the "test" user context

if [ "$USER" != "test" -a "$USER" != "pshaw" ]; then
    exit 0
fi
    
OS=`uname`

echo $1 | ssh volt3f "cat > libs/arg"
if [ "$OS" = "Darwin" -a -e obj/release/nativelibs/libvoltdb*.jnilib ]
then
    # the Mac case...
    tar cf - obj/release/nativelibs/ | ssh volt3e "cd libs; tar xf -"
else
    # the Linux case...
    
    echo "++++++++++++++Linux side"
    if [ -e ~/libs/obj/release/nativelibs/* ]; then
        mkdir -p $1
        cp ~/libs/obj/release/nativelibs/* $1
    else
        echo "++++++++++++++nativelibs not found!"
    fi
fi
