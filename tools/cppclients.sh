#/bin/sh

# get the directory containing this script
TOOLSDIR=`readlink -f $(dirname "${BASH_SOURCE[0]}")`

grep -A1000 \/\\\* README > clientvoter.cpp
`grep -A5 g++ README | sed 'N;N;s/[\n\\]//g' | sed 's/voltdb.[ul][si][rb].//'`
${TOOLSDIR}/cppclients.exp

cd AsyncHelloWorld/
make clean all

cd ../HelloWorld/
make clean all
