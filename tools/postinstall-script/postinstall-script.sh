# !/usr/bin/env bash
 
# script used to collect data
# ensure your PATH is configured to properly with voltdb
# script assumes you are on a running node or exits
 
cleanup() {
#cleanupdir before starting again
	if [ -a $1 ]
		then
		rm $1
	fi
}
 
checkvoltdbstatus() {
	if (( $( ps -ef | grep -v grep | grep -i voltdb | wc -l ) > 0 ))
		then
		echo "voltdb is running"
	else
		echo "voltdb is not running"
		echo "exiting"
		exit 1
	fi
}
 
formatoutput() {
	# remove java warnings, remove blank lines
	cat $1 | grep -v "Strict java memory checking is enabled" | sed '/^ *$/d' | sed 's/.*Returned.*//' >> $1-formatted
}
 
systeminfocheck() {
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
 
 
#####program start
#find old files in current directory and clean up
echo "clean up old files"
for f in $( ls -l postcheckup* | tr -s " " | cut -d' ' -f9 ); do
	cleanup $f
done
echo "checking for running voltdb"
checkvoltdbstatus
echo "collecting system information"
systeminfocheck
echo "collecting data on 1 minute intervals for 10 minutes"
sqlcmd --query="exec @SystemInformation OVERVIEW;exec @SystemInformation DEPLOYMENT;" >>postcheckup-systeminfo
for i in {1..10}; do 
# for i in {1..1}; do 
	( echo -n "interval $i, " & date & sqlcmd --output-format=csv --query="exec @Statistics, TABLE 1;" ) >>postcheckup-table 2>/dev/null
	( echo -n "interval $i, " & date & sqlcmd --output-format=csv --query="exec @Statistics, MEMORY 1;" ) >>postcheckup-memory 2>/dev/null
	( echo -n "interval $i, " & date & sqlcmd --output-format=csv --query="exec @Statistics, PROCEDUREPROFILE 1;" ) >>postcheckup-procprofile 2>/dev/null
	( echo -n "interval $i, " & date & sqlcmd --output-format=csv --query="exec @Statistics, LATENCY 1;" ) >>postcheckup-latency 2>/dev/null
	( echo -n "interval $i, " & date & sqlcmd --output-format=csv --query="exec @Statistics, CPU 1;" ) >>postcheckup-cpu 2>/dev/null
	sleep 60
done
echo "formatting data collected"
for f in $( ls -l postcheckup* | tr -s " " | cut -d' ' -f9 ); do
	formatoutput $f
done
echo "zipping files"
zip postcheckup.zip postcheckup-*formatted >/dev/null
echo "clean up tmp files"
for f in $( ls -l postcheckup* | grep -v zip | tr -s " " | cut -d' ' -f9 ); do
	cleanup $f
done

