# !/usr/bin/env bash

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# script used to collect data
# ensure your PATH is configured to properly with voltdb
# script assumes you are on a running node or exits

# standard usage function
function usage() {
	echo "Usage: $0 [--user username] [--password password] [--port portnumber]"
	echo "Description: postlaunch checkup script"
	echo "Command line parameters:"
	echo "  --user 			login username"
	echo "  --password		login password"
	echo "  --port			portnumber"
	echo ""
	if [ -n "$1" ]; then
		echo "*** ERROR: $1 ***"
		echo ""
	fi
	exit 1
}

function cleanup() {
#cleanupdir before starting again
	if [ -a $1 ]; then
		rm $1
	fi
}

function checkvoltdbstatus() {
	if (( $( jps | grep -c VoltDB ) > 0 )); then
		echo "voltdb is running"
	else
		echo "VOLTDB IS NOT RUNNING"
		echo "EXITING"
		exit 1
	fi
}

function formatoutput() {
# remove java warnings, remove blank lines
	cat $1 | grep -v "Strict java memory checking is enabled" | sed '/^ *$/d' | sed 's/.*Returned.*//' >> $1-formatted
}

function systeminfocheck() {
	hostname >>postcheckup-systeminfo
	uname -a >>postcheckup-systeminfo
	uptime >>postcheckup-systeminfo
	( df -h | xargs | awk '{print "Free disk " $11 " / Total disk " $9  " Percentfull " $12 }' ) >>postcheckup-systeminfo
	( free -m | xargs | awk '{print "Free memory " $17 " / Total memory " $8 " MB"}' ) >>postcheckup-systeminfo
   	( top -b | head -3 ) >>postcheckup-systeminfo
   	( top -b | head -10 | tail -4 )  >>postcheckup-systeminfo
	vmstat 1 5 >>postcheckup-systeminfo
	( echo "cpucore count: " & cat /proc/cpuinfo | grep processor | wc -l ) >>postcheckup-systeminfo
}

function testsqlcmd() {
	sqlcmd $@ --query="exec @SystemInformation OVERVIEW;" >/dev/null
	if [ "$?" -ne "0" ]; then
		echo "SQLCMD FAILED TO CONNECT - check your PATH variable"
		echo "EXITING"
		exit 1
	else
		echo "sqlcmd connected successfully"
	fi
}

function collectdata() {
	sqlcmd $@ --query="exec @SystemInformation OVERVIEW;exec @SystemInformation DEPLOYMENT;" >>postcheckup-systeminfo
	for i in {1..10}; do
		( echo -n "interval $i, " & date & sqlcmd $@ --output-format=csv --query="exec @Statistics, TABLE 0;" ) >>postcheckup-table 2>/dev/null
		( echo -n "interval $i, " & date & sqlcmd $@ --output-format=csv --query="exec @Statistics, MEMORY 0;" ) >>postcheckup-memory 2>/dev/null
		( echo -n "interval $i, " & date & sqlcmd $@ --output-format=csv --query="exec @Statistics, PROCEDUREPROFILE 0;" ) >>postcheckup-procprofile 2>/dev/null
		( echo -n "interval $i, " & date & sqlcmd $@ --output-format=csv --query="exec @Statistics, LATENCY 0;" ) >>postcheckup-latency 2>/dev/null
		( echo -n "interval $i, " & date & sqlcmd $@ --output-format=csv --query="exec @Statistics, CPU 0;" ) >>postcheckup-cpu 2>/dev/null
		sleep 60
	done
}

#####program start
USER=
PASSWORD=
ADMINPORT=
# check arguments
if [ -z "$1" ]; then
	echo "no arguments passed; no username/password/port provided"
else
	while [ -n "$1" ]; do
		if [ "$1" == "--user" ]; then
			if [ -z "$2" ]; then
				usage "You must specify an argument with $1!"
			else
				USER=$2
				shift
				shift
			fi
		elif [ "$1" == "--password" ]; then
			if [ -z "$2" ]; then
				usage "You must specify an argument with $1!"
			else
				PASSWORD=$2
				shift
				shift
			fi
		elif [ "$1" == "--port" ]; then
			if [ -z "$2" ]; then
				usage "You must specify an argument with $1!"
			else
				ADMINPORT=$2
				shift
				shift
			fi
		else
			usage "Please check usage"
		fi
	done
	echo "arguments passed - USER=$USER PASSWORD=$PASSWORD PORT=$ADMINPORT "
fi
#find old files in current directory and clean up
echo "clean up old files"
for f in $( ls -l postcheckup* 2>/dev/null | tr -s " " | cut -d' ' -f9 ); do
	cleanup $f
done
echo "checking for running voltdb"
checkvoltdbstatus
echo "collecting system information"
systeminfocheck
if [ -z "$USER" ] && [ -z "$PASSWORD" ]; then
	parameter=
elif [ -n "$USER" ] && [ -n "$PASSWORD" ]; then
	parameter="--user=$USER --password=$PASSWORD"
else
	echo "USER or PASSWORD not defined"
	echo "EXITING"
	exit 1
fi
if [ -n "$ADMINPORT" ]; then
	export parameter="$parameter --port=$ADMINPORT"
fi
echo "testing sqlcmd connection"
testsqlcmd $parameter
echo "collecting data on 1 minute intervals for 10 minutes"
collectdata $parameter

echo "formatting data collected"
for f in $( ls -l postcheckup* | tr -s " " | cut -d' ' -f9 ); do
	formatoutput $f
done
echo "zipping files"
zip postlaunch-checkup.zip postcheckup-*formatted >/dev/null
echo "clean up tmp files"
for f in $( ls -l postcheckup* | grep -v zip | tr -s " " | cut -d' ' -f9 ); do
	cleanup $f
done
echo "Please send zip file \"postlaunch-checkup.zip\" to support@voltdb.com"
echo "Script done; exiting"
