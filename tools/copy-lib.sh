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

if [ -z "$BUILD_ID" ]; then
    # no build id -- fail the build or go on without
    # the single kit capability?
    echo "No BUILD_ID: not single kit build -- unsuccessful"
    exit 0
fi

echo $1 | ssh volt3f "cat > libs/arg"
if [ "$OS" = "Darwin" -a -e obj/release/nativelibs/libvoltdb*.jnilib ]
then
    # the Mac case...
    tar cf - obj/release/nativelibs/ | ssh volt3e "mkdir -p libs/$BUILD_ID; cd libs/$BUILD_ID; tar xf -"
else
    # the Linux case...
   mkdir -p $1
   cp ~/libs/$BUILD_ID/obj/release/nativelibs/* $1
fi
