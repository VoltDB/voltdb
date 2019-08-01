#!/bin/sh -e

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# @author Lukai L.
# This script is meant for developer on Calcite integeration to VoltDB.
#
# Assuming that you have an Apache Calcite Git repo sharing the same root directory
# as voltdb, e.g. $HOME/voltdb/ and $HOME/calcite/, and the Git branch in your calcite
# directory is in a fully commited branch based on (or in) branch-1.17:
#
# This script shall validate these assumptions, and generated a Git patch file for
# all the changes you have made since the last official commit of branch-1.17, and
# trigger VoltDB Ant target `refresh_calcite' that: un-tars the official Calcite tar
# file, apply the patches to calcite repo just produced, build calcite and install
# the jar files in voltdb/lib directory. That way, you have synchronized all your
# changes in your local Apache Calcite to Voltdb, and you can start working on the
# VoltDB side immediately!
#
# This script also saves trouble on Calcite side of work, as no manual git patch
# generation is necessary at all.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

read_link() {    # Emulates GNU `readlink -f' behavior on Linux and MacOS.
    local path=$1
    if [ -d $path ] ; then
        local abspath=$(cd $path; pwd)
    else
        local dirname=$(cd $(dirname -- $path) ; pwd)
        local basename=$(basename $path)
        local abspath=$dirname/$basename
    fi
    echo $abspath
}

voltdb_dir=$(read_link $(dirname $0)/../)
calcite_dir=$(read_link $voltdb_dir/../calcite/)
if [ ! -d "$calcite_dir" ]; then       # Check location of calcite/ directory
   echo "Cannot find calcite repo in $calcite_dir"
   exit 1
fi
cd $calcite_dir
if [ $(git status | egrep -c 'modified|deleted') -ne 0 ]; then    # Check Git cleanness of the branch
   echo "Error: Calcite repo is not clean. Remember to commit/stash your changes before continue:"
   git status
   exit 2
fi
base_date="2018-07-16"     # Calcite 1.17 release date
commits=$(git log --after=$base_date --pretty=format:%h | wc -w)
if [ $commits -eq 0 ]; then         # Check that there are actually things to patch
   echo "No commit(s) found in current branch since 07/16/2018. Abort."
   exit 3
fi
base_commit=$(git log -1 --author='Volodymyr Vysotskyi' --pretty=format:%h)
last_author_date=$(git log -1 --pretty=format:%ai $base_commit | cut -d' ' -f1)
if [ "$last_author_date" != "$base_date" ]; then                    # Check that current branch is based on/in branch-1.17
   echo "Expect the last commit by $author should be on $base_date, found $last_author_date. "
   echo "Does current calcite branch fork from origin/branc-1.17??"
   exit 4
fi
git format-patch --stdout $base_commit..HEAD > $voltdb_dir/third_party/java/tar/commits.patch     # Generate single patch file
cp $voltdb_dir/third_party/java/tar/commits.patch /tmp
echo "Patch regenerated on $commits commits. The patch file can now be found in /tmp/commits.patch\n Running \`ant refresh_calcite'..."
# Run VoltDB Ant target
cd $voltdb_dir
ant refresh_calcite && echo "Now develop/build VoltDB with the latest Calcite version!"

