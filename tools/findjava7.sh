#!/bin/bash
#Find java7 home directory and writes it to stdout

version=`javac -version 2>&1`

if [[ $version == *"7"* ]]
then
    whichjavac=`which javac`
    readlink -m $whichjavac/../..
elif  [ -d '/usr/lib/jvm/java-1.7.0' ]
then
    jhome='/usr/lib/jvm/java-1.7.0'
    echo $jhome
    exit
fi
