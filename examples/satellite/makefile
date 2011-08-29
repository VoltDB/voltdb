CLASSPATH = ../../obj/release/prod:../../lib
VOLTDB = ../../bin/voltdb

.PHONY: main clean srccompile compile server client tracker

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm satellite.jar
	rm -rf voltdbroot

srccompile:
	mkdir -p obj
	javac -classpath $(CLASSPATH) -d obj \
		signal/*.java
		procedures/*.java \
		tracker/*.java

compile: srccompile
	$(VOLTCOMPILER) obj project.xml satellite.jar

server: compile
	$(VOLTDB) create catalog satellite.jar deployment deployment.xml

client: srccompile
	java -classpath obj:$(CLASSPATH) signal.Client
		5 \ # number of satellites
		usa # country

tracker: srccompile
	java -classpath obj:$(CLASSPATH) tracker.UserInterface
		5 \ # number of satellites
		usa # country