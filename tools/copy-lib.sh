#!/bin/bash

# ENG-8472: Add both Mac and Linux native libraries to the voltdb-<rel>.jar.
#  That way, we can release a single kit, whether tarball, .deb, or .rpm that
#  contains all the binaries to run on either platform
#
# For automated builds only; harmless in a dev build but adds no value
# so enabled only in the "test" user context

# Note: this will need additional work if there are simultaneous builds on the Mac side.

if [ "$USER" != "test" ]; then
    exit 0
fi

OS=`uname`

echo '$1 is' $1
echo '$2 is' $2
echo 'lib is' obj/release/nativelibs/libvoltdb*.jnilib

if [ "$OS" = "Darwin" -a -e obj/release/nativelibs/libvoltdb-${2}.jnilib ]
then
    # the Mac case...
    # clean up target directory on volt0, just to be safe
    # ssh volt0 "cd libs; rm -rf obj"

    # now copy the new native lib over to the linux side
    tar cvf - obj/release/nativelibs/ | ssh volt0 "cd libs; tar xf -"
    exit 0
else
    # the Linux case...
    echo "+++ (pre) target directory:"
    ls -lRtr $2
    if [ -e ~/libs/obj/release/nativelibs/libvoltdb-${2}.jnilib ]; then
        mkdir -p $1
        mv ~/libs/obj/release/nativelibs/libvoltdb-${2}.jnilib $1
        exit 0
    else
        echo "++++++++++++++nativelibs not found!"
        exit 0
    fi
    echo "+++ (post) target directory:"
    ls -lRtr $2
fi
