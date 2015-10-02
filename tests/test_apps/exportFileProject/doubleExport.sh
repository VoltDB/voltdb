
rows=1000
while [ $rows -lt 3000000 ]; do
    echo $rows
    sed -e "s/NAME/$rows/" deployment.xml.template > deployment.xml
    echo truncate table kvtable | sqlcmd
    voltadmin update deployment.xml
    java -classpath client.jar::/home/pshaw/voltdb/voltdb/voltdbclient-5.7beta1.jar:/home/pshaw/voltdb/lib/commons-cli-1.2.jar:/home/pshaw/voltdb/lib/commons-lang3-3.0.jar -Dlog4j.configuration=file:///home/pshaw/voltdb/voltdb/log4j.xml exportFileProject.client.exportFileProject.AsyncClient --displayinterval=5 --duration=30 --servers=volt5b,volt5c --rows=$rows
    rows=$(($rows * 2))
done

