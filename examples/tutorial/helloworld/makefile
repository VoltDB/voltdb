CLASSPATH = ../../../obj/release/prod:../../../lib
VOLTDB = ../../../bin/voltdb
VOLTCOMPILER = ../../../bin/voltcompiler

.PHONY: main clean srccompile compile server client

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm helloworld.jar
	rm -rf voltdbroot

srccompile:
	mkdir -p obj
	javac -classpath $(CLASSPATH) -d obj *.java

compile: srccompile
	$(VOLTCOMPILER) obj project.xml helloworld.jar

server: compile
	$(VOLTDB) create catalog helloworld.jar deployment deployment.xml

client: srccompile
	java -classpath obj:$(CLASSPATH) Client
