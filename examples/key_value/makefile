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
		999999999 \ # maximum transactions per second (used as starting point only when auto-tuning is on) NOTE: Rounded to thousands
		5 \         # feedback interval (seconds)
		120 \       # test duration after initialization (seconds)
		3 \         # seconds to wait before recording latency information
		localhost \ # servers to connect to
		50 \        # key size (max 250)
		12000 \     # min value size (max 1048576)
		12000 \     # max value size (max 1048576)
		100000 \    # number of initial keys/values to populate
		75 \        # percentage of gets (vs. puts) once populated
		2 \         # 1 = store payload, 2 = gzip payload
		true \      # Whether auto-tuning should be turned on, allowing the benchmark to determine an optimal TPS for a given target latency
		10.0 \      # Target average transaction call latency (in milliseconds)
		0.95 \      # Adjustment rate: the tuning process decreases the number of requested transactions per second to FACTOR*<observed-TPS> until the target latency is reached
		1 \         # Auto-tuning interval (in seconds)

client-threaded: srccompile
	java -classpath obj:$(CLASSPATH) com.ClientThreadedKV \
		999999999 \ # maximum transactions per second (no autu-tuning available for this client - use output from simple client as baseline )
		5 \         # feedback interval (seconds)
		120 \       # test duration after initialization (seconds)
		3 \         # seconds to wait before recording latency information
		localhost \ # servers to connect to
		50 \        # key size (max 250)
		12000 \     # min value size (max 1048576)
		12000 \     # max value size (max 1048576)
		100000 \    # number of initial keys/values to populate
		75 \        # percentage of gets (vs. puts) once populated
		2 \         # 1 = store payload, 2 = gzip payload