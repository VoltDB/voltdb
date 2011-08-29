CLASSPATH = ../../obj/release/prod:../../lib
VOLTDB = ../../bin/voltdb
VOLTCOMPILER = ../../bin/voltcompiler

.PHONY: main clean srccompile compile server client sync-client

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm voter.jar
	rm -rf voltdbroot

srccompile:
	mkdir -p obj
	javac -classpath $(CLASSPATH) -d obj \
		src/com/*.java \
		src/com/procedures/*.java

compile: srccompile
	$(VOLTCOMPILER) obj project.xml voter.jar

server: compile
	$(VOLTDB) create catalog voter.jar deployment deployment.xml

client: srccompile
	java -classpath obj:$(CLASSPATH):obj com.ClientVoter \
		6 \         # total number of contestants (maximum 12)
		2 \         # number of votes allowed per phone number
		1000000 \   # maximum number of votes per second this client can generate (used as starting point only when auto-tuning is on) NOTE: Rounded to thousands
		5 \         # client application feedback interval (seconds)
		120 \       # client application duration (seconds)
		3 \         # number of seconds to wait before recording latency information
		localhost \ # comma separated list of servers to connect to
		true        # Whether auto-tuning should be turned on, allowing the benchmark to determine an optimal TPS for a given target latency
		10.0        # Target average transaction call latency (in milliseconds)
		0.99        # Adjustment rate: the tuning process decreases the number of requested transactions per second to FACTOR*<observed-TPS> until the target latency is reached
		2           # Auto-tuning interval (in seconds)

sync-client: srccompile
	java -classpath obj:$(CLASSPATH):obj com.SyncClientVoter \
		6 \         # total number of contestants (maximum 12)
		2  \        # number of votes allowed per phone number
		5000 \      # maximum number of votes per second this client can generate
		5 \         # client application feedback interval (seconds)
		120 \       # client application duration (seconds)
		3 \         # number of seconds to wait before recording latency information
		localhost \ # comma separated list of servers to connect to