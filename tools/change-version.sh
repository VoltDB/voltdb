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

if [ "$(uname)" == "Darwin" ];
    then
    SED="sed -i ''"
    else
    SED="sed -i"
    fi


echo $SED
showchange() {
    echo ""
    echo "Changes in " $file
    git diff $file
}

#Change version.txt
file="version.txt"
echo $version > $file
showchange

# Change RealVoltDB.java
file="src/frontend/org/voltdb/RealVoltDB.java"
$SED "s/static final String m_defaultVersionString = \".*\"/static final String m_defaultVersionString = \"${version}\"/" $file
#8 backslashes allows bash, then sed to do their thing,
#leaving double-backslash in the actual file
$SED "s/static final String m_defaultHotfixableRegexPattern = \".*\"/static final String m_defaultHotfixableRegexPattern = \"^\\\\\\\\Q${version}\\\\\\\\E\\\\\\\\z\"/" $file
showchange


# Change verify_kits.py
file="tools/kit_tools/verify_kits.py"
$SED "s/version = \".*\"/version = \"${version}\"/" $file
showchange

# Change mainui.js
file="src/frontend/org/voltdb/studio/js/mainui.js"
$SED "s/var \$volt_version = '.*'/var \$volt_version = '${version}'/" $file
showchange

