CLASSPATH = ../../obj/release/prod:../../lib
VOLTDB = ../../bin/voltdb
VOLTCOMPILER = ../../bin/voltcompiler

.PHONY: main clean srccompile compile server client

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm -rf voltcache.jar
	rm -rf plannerlog.txt
	rm -rf voltdbroot

srccompile:
	mkdir -p obj
	javac -classpath $(CLASSPATH) -d obj \
		src/com/*.java \
		src/com/api/*.java \
		src/com/procedures/*.java

compile: srccompile
	$(VOLTCOMPILER) obj project.xml voltcache.jar

server: compile
	$(VOLTDB) create catalog voltcache.jar deployment deployment.xml

client: benchmark

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
benchmark-help: srccompile
	java -classpath obj:$(CLASSPATH):obj com.Benchmark \
		--help

benchmark: srccompile
	java -classpath obj:$(CLASSPATH):obj com.Benchmark \
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

