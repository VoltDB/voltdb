CLASSPATH = ../../obj/release/prod:../../lib
VOLTDB = ../../bin/voltdb
VOLTCOMPILER = ../../bin/voltcompiler

.PHONY: main clean srccompile compile server client gui

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm gameoflife.jar
	rm -rf voltdbroot

srccompile:
	mkdir -p obj
	javac -classpath $(CLASSPATH) -d obj \
		src/game/db/*.java
		src/game/drivers/*.java
		src/game/gui/*.java
		src/game/procedures/*.java
		src/game/*.java

compile: srccompile
	$(VOLTCOMPILER) obj project.xml gameoflife.jar

server: compile
	$(VOLTDB) create catalog gameoflife.jar deployment deployment.xml

client: srccompile
	java -classpath obj:$(CLASSPATH) game.drivers.GameDriver
		100 /       # number of rows
		100 /       # number of columns
		4000 /      # initial population
		0 /         # delay in milliseconds between board evolutions
		localhost / # comma separated list of servers to connect to

gui: srccompile
	java -classpath obj:$(CLASSPATH) game.drivers.GUIDriver
		500 /       # delay in milliseconds between board repaints
		localhost / # comma separated list of servers to connect to