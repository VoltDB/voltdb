echo 'dollar star is: ' $*
rows=`cat $*/* | wc -l`
tmpfile=`mktemp`
echo "sort -u $*/* to $tmpfile"
sort -u $*/* > $tmpfile
dedupedrows=`cat $tmpfile | wc -l`
duperows=$(($rows - $dedupedrows))
echo Dupes: $duperows
python 123-nodupcheck.py $tmpfile
rm -f $tmpfile
