#!/bin/bash
#
# ./tools/change_version.sh version#
#
# Changes the 3 files that need the version string
# Must be run from voltdb top-level directory

if [ $# != 1 ]
    then
    echo "Usage: change_version.sh version#"
    exit 1
    fi

if [ ! -f ./version.txt ]
    then
    echo "This must be run from the voltdb top-level directory"
    exit 1
    fi

version=$1

showchange() {
    echo ""
    echo "Changed $file:"
    grep $version $file
}

#Change version.txt
file="version.txt"
echo $version > $file
showchange

# Change RealVoltDB.java
file="src/frontend/org/voltdb/RealVoltDB.java"
sed -i ''  "s/m_defaultVersionString = \".*\"/m_defaultVersionString = \"${version}\"/" $file
showchange

# Change verify_kits.py
file="tools/kit_tools/verify_kits.py"
sed -i '' "s/version = \".*\"/version = \"${version}\"/" $file
showchange

# Change mainui.js
file="src/frontend/org/voltdb/studio/js/mainui.js"
sed -i '' "s/var \$volt_version = '.*'/var \$volt_version = '${version}'/" $file
showchange

