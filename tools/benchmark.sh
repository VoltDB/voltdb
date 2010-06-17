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

echo "cd ~/trunk && BMCLIENT=$BMCLIENT BMDURATION=$BMDURATION BMCLIENTCOUNT=1 FIRSTCLIENT=$HOSTNAME BMKFACTOR=$BMKFACTOR $EXTRAENV BMSITESPERHOST=12 BMTPCCWAREHOUSES=`dc -e "$BMHOSTCOUNT 12 * p q"` VOLTBIN=/home/test/workspace/build-centos/trunk/obj/release/voltbin BMHOSTCOUNT=$BMHOSTCOUNT ant benchmarkenv" > ~/trunk/tools/benchmark-$HOSTNAME.sh
chmod a+x ~/trunk/tools/benchmark-$HOSTNAME.sh

rm -f /home/test/output/benchmark-$HOSTNAME.log
touch /home/test/output/benchmark-$HOSTNAME.log
tail -f /home/test/output/benchmark-$HOSTNAME.log &
java -jar ~/.hudson/hudson-cli.jar -s http://newbobbi:8080 build $MULTINODE_HELPER -p COMMAND="~/trunk/tools/benchmark-$HOSTNAME.sh" -p COUNT=$BMHOSTCOUNT -p HOST2=empty -p HOST3=empty -p HOST4=empty -p HOST5=empty -p HOST6=empty -p OUTPUTFILE=/home/test/output/benchmark-$HOSTNAME.log -s
