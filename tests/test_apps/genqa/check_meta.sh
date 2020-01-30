# echo $1
rows=`cat $1/* | wc -l`
tmpfile=`mktemp`
# echo "sort -u $1/* to $tmpfile"
sort -u $1/* > $tmpfile
dedupedrows=`cat $tmpfile | wc -l`
duperows=$(($rows - $dedupedrows))
echo Dupes: $duperows
python 123-nodupcheck.py $tmpfile
rm -f $tmpfile
