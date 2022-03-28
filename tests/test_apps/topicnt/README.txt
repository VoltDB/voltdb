Simple test using NT procedures in a topic, allowing invoking more than 1 procedure, partitioned differently.

Test scenario:

(run the test)

./run.sh clean
./run.sh server
./run.sh init
./run.sh client

(check results)

kafka-console-consumer --bootstrap-server localhost:9092 --topic next_actions --from-beginning --property print.key=true
kafka-console-consumer --bootstrap-server localhost:9092 --topic user_hits --from-beginning --property print.key=true
kafka-console-consumer --bootstrap-server localhost:9092 --topic account_hits --from-beginning --property print.key=true
kafka-console-consumer --bootstrap-server localhost:9092 --topic cookie_errors --from-beginning --property print.key=true

(exit volt then cleanup)

voltadmin shutdown or kill -9
./run.sh clean
