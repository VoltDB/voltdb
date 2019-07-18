#!/bin/bash

# ENG-8472: Add both Mac and Linux native libraries to the voltdb-<rel>.jar.
#  That way, we can release a single kit, whether tarball, .deb, or .rpm that
#  contains all the binaries to run on either platform
#
# For automated builds only; harmless in a dev build but adds no value
# so enabled only in the "test" user context, and if the argument
# "kitbuild", set as a Ant property, is true

if [ "$USER" != "test" -o "$3" = "false" ]; then
    echo "Skipping copy-lib checking and copy"
    exit 0
fi

echo "Arguments from Volt build (build.xml)"
echo -e "\t Native lib location:" $1
echo -e "\t Version:" $2
echo -e "\t Kitbuild:" $3

OS=`uname`

if [ "$OS" = "Darwin" ]
then
    # the Mac case...
    if [ -e obj/release/nativelibs/libvoltdb-$2.jnilib ]
    then
        tar cf - obj/release/nativelibs/ | ssh volt0 "cd nativelibs; tar xf -"
        exit 0
    else
        echo "Native lib copy from Mac build to volt0 failed."
        exit 1
    fi
else
    # the Linux case...
    if [ -e ~/nativelibs/obj/release/nativelibs/libvoltdb-$2.jnilib ]; then
        mkdir -p $1
        cp ~/nativelibs/obj/release/nativelibs/libvoltdb-$2.jnilib $1
        cp ~/nativelibs/obj/release/nativelibs/libcatalog-$2.jnilib $1
        exit 0
    else
        echo "Mac native lib expected -- not found!"
        exit 1
    fi
fi
