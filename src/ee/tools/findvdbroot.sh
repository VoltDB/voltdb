#!/bin/bash
function isroot() {
    if [ ! -f build.xml ]; then
        return 1
    fi
    if grep '<project' build.xml | grep -q 'name="VoltDB"'; then
        return 0
    fi
    return 1;
}

while ! isroot || ! [ "$(/bin/pwd)" != "/" ]; do
    cd ..
done
if [ "$(/bin/pwd)" = "/" ] ; then
    exit 1
else
    /bin/pwd
fi
