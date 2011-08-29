CLASSPATH = ../../obj/release/prod:../../lib
VOLTDB = ../../bin/voltdb
VOLTCOMPILER = ../../bin/voltcompiler

.PHONY: main clean srccompile compile server client client-threaded

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm key_value_catalog.jar
	rm -rf voltdbroot

srccompile:
	mkdir -p obj
	javac -classpath $(CLASSPATH) -d obj \
		src/com/*.java \
		src/com/procedures/*.java

compile: srccompile
	$(VOLTCOMPILER) obj project.xml key_value_catalog.jar

server: compile
	$(VOLTDB) create catalog key_value_catalog.jar deployment deployment.xml

client: srccompile
	java -classpath obj:$(CLASSPATH) com.ClientKV \
		999999999 \
		5 \
		120 \
		3 \
		localhost \
		50 \
		12000 \
		12000 \
		100000 \
		75 \
		2 \
		true \
		10.0 \
		0.95 \
		1

# CLIENT ARGS IN ORDER
# maximum transactions per second (used as starting point only when auto-tuning is on) NOTE: Rounded to thousands
# feedback interval (seconds)
# test duration after initialization (seconds)
# seconds to wait before recording latency information
# servers to connect to
# key size (max 250)
# min value size (max 1048576)
# max value size (max 1048576)
# number of initial keys/values to populate
# percentage of gets (vs. puts) once populated
# 1 = store payload, 2 = gzip payload
# Whether auto-tuning should be turned on, allowing the benchmark to determine an optimal TPS for a given target latency
# Target average transaction call latency (in milliseconds)
# Adjustment rate: the tuning process decreases the number of requested transactions per second to FACTOR*<observed-TPS> until the target latency is reached
# Auto-tuning interval (in seconds)


client-threaded: srccompile
	java -classpath obj:$(CLASSPATH) com.ClientThreadedKV \
		999999999 \
		5 \
		120 \
		3 \
		localhost \
		50 \
		12000 \
		12000 \
		100000 \
		75 \
		2

# CLIENT-THREADED ARGS IN ORDER
# maximum transactions per second (no autu-tuning available for this client - use output from simple client as baseline )
# feedback interval (seconds)
# test duration after initialization (seconds)
# seconds to wait before recording latency information
# servers to connect to
# key size (max 250)
# min value size (max 1048576)
# max value size (max 1048576)
# number of initial keys/values to populate
# percentage of gets (vs. puts) once populated
# 1 = store payload, 2 = gzip payload
