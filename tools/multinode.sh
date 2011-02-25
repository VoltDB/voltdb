# clean up snapshots from dead benchmarks here, so that there's always
# exactly one set per machine
rm -f /tmp/volt_snapshot_*

if [ $COUNT -eq 1 ]; then
  # log the version number so it works on machines with wrong svn version too
  grep "trunk$" ~/trunk/.svn/all-wcprops > $OUTPUTFILE
  # execute the command
  echo at time: `date`, command: $COMMAND
  HOST1=`hostname` eval $COMMAND 2>&1 | tee -a $OUTPUTFILE
  RET=$?
  # wait for possible NFS delays so that all output flushes to calling console
  echo at time: `date`, waiting 60 seconds for completion of command: $COMMAND
  sleep 60
  echo at time: `date`, cleaning up
  # kill calling console's tail -f process
  cp $OUTPUTFILE ${OUTPUTFILE}.bak
  rm $OUTPUTFILE
  exit $RET
else
  # recurse, shifting HOST environment variables
  if [ $COUNT -eq 2 ]; then
    SUFFIX="-timeout"
  fi
  echo at time: `date`, command: $COMMAND
  java -jar ~/.hudson/hudson-cli.jar -s ${HUDSON_URL} build ${JOB_NAME}${SUFFIX} -p COMMAND="$COMMAND" -p COUNT=`expr $COUNT - 1` -p HOST2=`hostname` -p HOST3=$HOST2 -p HOST4=$HOST3 -p HOST5=$HOST4 -p HOST6=$HOST5 -p HOST7=$HOST6 -p HOST8=$HOST7 -p HOST9=$HOST8 -p HOST10=$HOST9 -p HOST11=$HOST10 -p HOST12=$HOST11 -p OUTPUTFILE=$OUTPUTFILE -s
  RET=$?
  echo at time: `date`, cleaning up
  exit $RET
fi
