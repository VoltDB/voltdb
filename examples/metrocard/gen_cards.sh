#0,1,0,5000,"2020-01-01 12:00:00",Rider1,7815551212,Rider1@test.com,0
#5000,2000,1000,500
max=500000
i=0
while true
do
echo $i",1,0,"100000",2020-01-01 12:00:00,Rider"$i",7815551212,Rider"$i"@test.com,"0
i=`expr $i + 1`
echo $i",1,0,"50000",2020-01-01 12:00:00,Rider"$i",7815551212,Rider"$i"@test.com,"0
i=`expr $i + 1`
echo $i",1,0,"5000",2020-01-01 12:00:00,Rider"$i",7815551212,Rider"$i"@test.com,"0
i=`expr $i + 1`
echo $i",1,0,"2000",2020-01-01 12:00:00,Rider"$i",7815551212,Rider"$i"@test.com,"0
i=`expr $i + 1`
echo $i",1,0,"1000",2020-01-01 12:00:00,Rider"$i",7815551212,Rider"$i"@test.com,"0
i=`expr $i + 1`
echo $i",1,0,"500",2020-01-01 12:00:00,Rider"$i",7815551212,Rider"$i"@test.com,"0
i=`expr $i + 1`
if [ $i -gt $max ]
then
    exit 0
fi
done
