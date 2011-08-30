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

client: async-benchmark

# Asynchronous benchmark sample
# Use this target for argument help
async-benchmark-help: srccompile
	java -classpath obj:$(CLASSPATH):obj com.AsyncVoterBenchmark \
	    --help

async-benchmark: srccompile
	java -classpath obj:$(CLASSPATH):obj com.AsyncVoterBenchmark \
	    --display-interval=5 \
	    --duration=120 \
	    --servers=localhost \
	    --port=21212 \
	    --contestants=6 \
	    --max-votes=2 \
	    --rate-limit=100000 \
	    --auto-tune=true \
	    --latency-target=10.0

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
sync-benchmark-help: srccompile
	java -classpath obj:$(CLASSPATH):obj com.SyncVoterBenchmark \
	    --help

sync-benchmark: srccompile
	java -classpath obj:$(CLASSPATH):obj com.SyncVoterBenchmark \
	    --threads=40 \
	    --display-interval=5 \
	    --duration=120 \
	    --servers=localhost \
	    --port=21212 \
	    --contestants=6 \
	    --max-votes=2

# JDBC benchmark sample
# Use this target for argument help
jdbc-benchmark-help: srccompile
	java -classpath obj:$(CLASSPATH):obj com.JDBCVoterBenchmark \
	    --help

jdbc-benchmark: srccompile
	java -classpath obj:$(CLASSPATH):obj com.JDBCVoterBenchmark \
	    --threads=40 \
	    --display-interval=5 \
	    --duration=120 \
	    --servers=localhost \
	    --port=21212 \
	    --contestants=6 \
	    --max-votes=2


