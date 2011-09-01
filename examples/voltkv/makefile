CLASSPATH = ../../obj/release/prod:../../lib
VOLTDB = ../../bin/voltdb
VOLTCOMPILER = ../../bin/voltcompiler
LICENSE = ../../voltdb/license.xml

.PHONY: main clean srccompile compile server client client-threaded

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm -f key_value_catalog.jar
	rm -rf plannerlog.txt
	rm -rf voltdbroot

srccompile:
	mkdir -p obj
	javac -classpath $(CLASSPATH) -d obj \
		src/com/*.java \
		src/com/procedures/*.java

compile: srccompile
	$(VOLTCOMPILER) obj project.xml key_value_catalog.jar

server: compile
	$(VOLTDB) create catalog key_value_catalog.jar \
		deployment deployment.xml license $(LICENSE) \
		leader localhost

client: async-benchmark

# Asynchronous benchmark sample
# Use this target for argument help
async-benchmark-help: srccompile
	java -classpath obj:$(CLASSPATH):obj com.AsyncBenchmark \
		--help

async-benchmark: srccompile
	java -classpath obj:$(CLASSPATH):obj com.AsyncBenchmark \
		--display-interval=5 \
		--duration=120 \
		--servers=localhost \
		--port=21212 \
		--pool-size=100000 \
		--preload=true \
		--get-put-ratio=0.90 \
		--key-size=32 \
		--min-value-size=1024 \
		--max-value-size=1024 \
		--use-compression=false \
		--rate-limit=100000 \
		--auto-tune=true \
		--latency-target=10.0


# Multi-threaded synchronous benchmark sample
# Use this target for argument help
sync-benchmark-help: srccompile
	java -classpath obj:$(CLASSPATH):obj com.SyncBenchmark \
		--help

sync-benchmark: srccompile
	java -classpath obj:$(CLASSPATH):obj com.SyncBenchmark \
		--threads=40 \
		--display-interval=5 \
		--duration=120 \
		--servers=localhost \
		--port=21212 \
		--pool-size=100000 \
		--preload=true \
		--get-put-ratio=0.90 \
		--key-size=32 \
		--min-value-size=1024 \
		--max-value-size=1024 \
		--use-compression=false

# JDBC benchmark sample
# Use this target for argument help
jdbc-benchmark-help: srccompile
	java -classpath obj:$(CLASSPATH):obj com.JDBCBenchmark \
	    --help

jdbc-benchmark: srccompile
	java -classpath obj:$(CLASSPATH):obj com.JDBCBenchmark \
		--threads=40 \
		--display-interval=5 \
		--duration=120 \
		--servers=localhost \
		--port=21212 \
		--pool-size=100000 \
		--preload=true \
		--get-put-ratio=0.90 \
		--key-size=32 \
		--min-value-size=1024 \
		--max-value-size=1024 \
		--use-compression=false

