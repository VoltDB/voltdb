# required environment variables:
#   BMCLIENT         The client to run.  Defaults to TPCC.
#   BMDURATION       The number of seconds for which to run the benchmark.
#   BMHOSTCOUNT      How many servers to use.
#   BMKFACTOR        The k-safety factor to use.
#   MULTINODE_HELPER The name of a multinode helper job with an appropriate
#                    timeout, such as multinode-CentOS-90m.
# optional environment variables:
#   EXTRAENV         A string containing extra options, typically used for
#                    snapshot setup or rate limiting.

# get the directory containing this script
TOOLSDIR=`readlink -f $(dirname "${BASH_SOURCE[0]}")`
# infer the trunk directory from that
TRUNKDIR=`dirname ${TOOLSDIR}`

echo "cd ${TRUNKDIR} && BMCLIENT=$BMCLIENT BMDURATION=$BMDURATION BMCLIENTCOUNT=1 CLIENT1=$HOSTNAME BMKFACTOR=$BMKFACTOR BMSITESPERHOST=12 BMTPCCWAREHOUSES=`dc -e "$BMHOSTCOUNT 12 * p q"` VOLTBIN=${TRUNKDIR}/obj/release/voltbin VOLTROOT=/home/test/voltdbroot BMHOSTCOUNT=$BMHOSTCOUNT $EXTRAENV ant benchmarkenv -Dchecktables=true" > ${TOOLSDIR}/benchmark-$HOSTNAME.sh
echo "date" >> ${TOOLSDIR}/benchmark-$HOSTNAME.sh
chmod a+x ${TOOLSDIR}/benchmark-$HOSTNAME.sh

if [ ! -f ${TRUNKDIR}/obj/release/voltbin/mysqlp -a -f ~/voltbin/mysqlp ]
then
    cp ~/voltbin/mysqlp ${TRUNKDIR}/obj/release/voltbin
fi

rm -f /home/test/output/benchmark-$HOSTNAME.log
rm -f /home/test/output/benchmark-$HOSTNAME.log.bak
touch /home/test/output/benchmark-$HOSTNAME.log
tail -f /home/test/output/benchmark-$HOSTNAME.log &
java -jar ~/.hudson/hudson-cli.jar -s http://newbobbi:8080 build $MULTINODE_HELPER -p COMMAND="${TOOLSDIR}/benchmark-$HOSTNAME.sh" -p COUNT=$BMHOSTCOUNT -p HOST2=empty -p HOST3=empty -p HOST4=empty -p HOST5=empty -p HOST6=empty -p HOST7=empty -p HOST8=empty -p HOST9=empty -p HOST10=empty -p HOST11=empty -p HOST12=empty -p OUTPUTFILE=/home/test/output/benchmark-$HOSTNAME.log -s || true
grep "BENCHMARK RESULTS" /home/test/output/benchmark-$HOSTNAME.log.bak
