#!/usr/bin/env bash

TESTS=${1:-"adperformance bank-offers callcenter contentionmark geospatial json-sessions metrocard nbbo positionkeeper uniquedevices voltkv voter windowing windowing-with-ddl"}

function echodo() {
  local opt_n=0 opt_s=0 opt_x=0 retval=0 opt=0 OPTIND
  while getopts "nsxh" opt ; do
    case $opt in
      n) opt_n=1 ;;
      x) opt_x=1 ;;
      s) opt_s=1 ;;
      h) echo "usage: echodo [-h|--help] [-n] [-s] [-x] args..." ;
         echo "    -n -> no-execute (echo-only)" ;
         echo "    -s -> silent (no-echo)" ;
         echo "    -x -> exit-on-error" ;
         exit 0
    esac
  done
  shift $((OPTIND-1))
  if [ "$1" = "--help" ]
  then
    echo "usage: echodo [-h|--help] [-n] [-s] [-x] args..."
    echo "    -n -> no-execute (echo-only)"
    echo "    -s -> silent (no-echo)"
    echo "    -x -> exit-on-error"
    exit 0
  fi

  if [ $opt_s -eq 0 ]
  then
    echo $@
  fi
  if [ $opt_n -eq 0 ]
  then
    $@
    retval=$?
    echo "+++ Return value: " $retval
    if [ \( $opt_x -eq 1 \) -a \( $retval -ne 0 \) ]
    then
      echo "exiting: '$@' returned: $retval"
      exit $retval
    fi
    return $retval
  fi
}

if [ `basename $PWD` != 'examples' ]; then
    echo Change to voltdb/examples directory and try again
    exit 1
fi
echo Running $TESTS

# in case there's a DB running...
echodo voltadmin shutdown
echodo sleep 5

for proj in $TESTS
do
    echodo pushd $proj
    echodo ./run.sh server &
    echodo sleep 20
    echodo ./run.sh init
    echodo ./run.sh client
    if [ \( "$proj" = "voltkv" \) -o \( "$proj" = "voter" \) ]; then
        echodo ./run.sh jdbc-benchmark
        echodo ./run.sh sync-benchmark
    fi
    echodo voltadmin shutdown
    echodo sleep 5
    echodo ./run.sh clean
    echodo ./run.sh cleanall
    echodo popd
done

