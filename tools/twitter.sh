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
links -source http://localhost:12345
# links leaves a line unterminated
echo
ps -ef | grep ant.jar | awk " { print \$2 } " | xargs kill -9
