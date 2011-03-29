#/bin/sh

# get the directory containing this script
TOOLSDIR=`readlink -f $(dirname "${BASH_SOURCE[0]}")`

# there's embedded code in the readme.
# try to compile it; readmes should be correct.
grep -A1000 \/\\\* README > clientvoter.cpp
# there's no makefile, but there is a c++ command in the readme.  Run it.
`grep -A5 g++ README | sed 'N;N;s/[\n\\]//g' | sed 's/voltdb.[ul][si][rb].//'`

cd AsyncHelloWorld/
make clean all

cd ../HelloWorld/
make clean all

cd ..
${TOOLSDIR}/cppclients.exp
