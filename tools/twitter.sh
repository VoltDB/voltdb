TOOLSDIR=`readlink -f $(dirname "${BASH_SOURCE[0]}")`
TRUNKDIR=`dirname ${TOOLSDIR}`
cd $TRUNKDIR
ant dist
cd obj/release/dist/examples/twitter
ant catalog
ant &
sleep 60
ant client -DTwitterUsername=$TUNAME -DTwitterPassword=$TUPASS &
sleep 20
ant cull &
sleep 180
OUTPUT=$(links -source http://localhost:12345 | grep hashtag-count | wc -l)
# links leaves a line unterminated
echo
ps -ef | grep ant.jar | awk " { print \$2 } " | xargs kill -9
test $OUTPUT -gt 0 && exit 0 || exit 1

