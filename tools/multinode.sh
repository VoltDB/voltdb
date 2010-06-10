# clean up snapshots from dead benchmarks here, so that there's always
# exactly one set per machine
rm -f /tmp/volt_snapshot_*

if [ $COUNT -eq 1 ]; then
  # execute the command
  HOST1=`hostname` eval $COMMAND 2>&1 > $OUTPUTFILE
  # wait for possible NFS delays so that all output flushes to calling console
  sleep 15
  # kill calling console's tail -f process
  rm $OUTPUTFILE
else
  # recurse, shifting HOST environment variables
  java -jar ~/.hudson/hudson-cli.jar -s ${HUDSON_URL} build ${JOB_NAME} -p COMMAND="$COMMAND" -p COUNT=`expr $COUNT - 1` -p HOST2=`hostname` -p HOST3=$HOST2 -p HOST4=$HOST3 -p HOST5=$HOST4 -p HOST6=$HOST5 -p OUTPUTFILE=$OUTPUTFILE -s
fi
