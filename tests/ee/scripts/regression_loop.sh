#!/bin/bash
export APPRUNNER_CLIENT_CLASSPATH=/home/opt/connection-pool/tomcat-jdbc.jar:/home/opt/connection-pool/tomcat-juli.jar:/home/opt/connection-pool/mchange-commons-java-0.2.10.jar:/home/opt/connection-pool/c3p0-0.9.5.1.jar:/home/opt/connection-pool/c3p0-oracle-thin-extras-0.9.5.1.jar:/home/opt/connection-pool/bonecp-0.8.0.RELEASE.jar:/home/opt/connection-pool/HikariCP-2.4.0.jar:/home/opt/connection-pool/guava-19.0-rc1.jar:/home/opt/connection-pool/slf4j-api-1.6.2.jar:/home/opt/connection-pool/slf4j-jdk14-1.6.2.jar:/home/opt/connection-pool/slf4j-nop-1.6.2.jar:/home/opt/kafka/libs/zkclient-0.3.jar:/home/opt/kafka/libs/zookeeper-3.4.6.jar:/home/opt/rabbitmq/rabbitmq.jar:/home/test/jdbc/vertica-jdbc.jar:/home/test/jdbc/postgresql-9.4.1207.jar

i=1
while true
do
 echo "run ee regression suit"
./apprunner.py -w Recover-Txnid2 --name=Recovertxnid2-$i --output=savedlogs --config=/var/lib/apprunner/configs/../config/randomize.py --duration=900 --nostats --commandlogging=on | tee log-regression-$i.txt
tail log-regression-$i.txt >> log-regression-end.txt
let i=$i+1;
./apprunner.py -w Restore-Recover --name=Restore-recover-s-$i --output=savedlogs --config=/var/lib/apprunner/configs/../config/1node.py,config/interfaces-office.py --commandlogsync --multisingleratio=0.001 |tee log-regression-$i.txt

tail log-regression-$i.txt >> log-regression-end.txt
let i=$i+1;
./apprunner.py -w Recover-Txnid2 --name=Endurance-Recovertxnid2 --output=savedlogs --commandlogging=on --duration=5973 --disabledthreads=partCappedlt,replCappedlt --config=/var/lib/apprunner/configs/../config/randomize.py --nostats

tail log-regression-$i.txt >> log-regression-end.txt
let i=$i+1;
./apprunner.py -w Rejoin2 --name=Endurance-DR-Rejoin2-xdcr-hmo-$i --output=savedlogs --nostats --regression-kit=build --replica-kit=build --schemachangethread=disable --ksafe --duration=9000 --commandlogging=choose --commandlogmode=choose --tweakcluster=choose --active-active=on --license=/home/test/AppRunner-2025-01-01-v2.xml --randomize --serverpool=volt19ac1,volt19ac2,volt19ac3,volt19ac4,volt19ac5,volt19ac6,volt19ac7,volt19ac8,volt19ac9,volt19ac10,volt19ac11,volt19ac12 --clusters=3 --disabledthreads=partCappedlt,replCappedlt --nopartitiondetection --swapratio=0 | tee log-regression-$i.txt
tail log-regression-$i.txt >> log-regression-end.txt
let i=$i+1;
./apprunner.py -w RejoinDeletes --name=Rejoin-deletes-3nk2-$i --duration=900 --ksafe --nopartitiondetection --config=/var/lib/apprunner/configs/../config/3node-k2.py --rejoin=choose | tee log-regression-rd-$i.txt
tail log-regression-rd-$i.txt >> log-regression-end.txt
let i=$i+1;

./apprunner.py -w Join-DR --name=join-txind2-dr-aa-cons-het-$i --no-switchlicenses --output=savedlogs --nostats --regression-kit=build --replica-kit=build --schemachangethread=disable --duration=600 --cluster=--role=regression --nodes=3 --cluster=--role=replica --randomize --elastic --commandlogging=on --commandlogmode=sync --randomize --disabledthreads=partCappedlt,replCappedlt,updateclasses --nopartialcluster --heterotopology --shutdownmode=kill9 --nopartitiondetection --swapratio=0 --active-active=on | tee log-regression-$i.txt
tail log-regression-$i.txt >> log-regression-end.txt

let i=$i+1;
done
