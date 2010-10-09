TOOLSDIR=`readlink -f $(dirname "${BASH_SOURCE[0]}")`
TRUNKDIR=`dirname ${TOOLSDIR}`
cd $TRUNKDIR
ln -sf obj/release/prod/voltdb .
cd examples/twitter
ant &
sleep 60
ant client &
sleep 20
ant cull &
sleep 180
links -source http://localhost:12345
ps -ef | grep ant.jar | awk " { print \$2 } " | xargs kill -9
