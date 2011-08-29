CLASSPATH = ../../obj/release/prod:../../lib
VOLTDB = ../../bin/voltdb
VOLTCOMPILER = ../../bin/voltcompiler

TWITTER_USER = YourUsernameHere
TWITTER_PASS = YourPasswordHere

.PHONY: main clean srccompile compile server client export hadoop-mr

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm twitter.jar
	rm -rf voltdbroot

srccompile:
	mkdir -p obj
	cp src/com/auctionexample/datafiles/*.txt obj/com/auctionexample/datafiles/
	javac -classpath $(CLASSPATH) -d obj \
		src/com/procedures/*.java \
		src/org/voltdb/twitter/database/*.java \
		src/org/voltdb/twitter/drivers/*.java \
		src/org/voltdb/twitter/hadoop/hdfs/*.java \
		src/org/voltdb/twitter/hadoop/mr/*.java \
		src/org/voltdb/twitter/server/*.java \
		src/org/voltdb/twitter/util/*.java

compile: srccompile
	$(VOLTCOMPILER) obj project.xml twitter.jar

server: compile
	$(VOLTDB) create catalog twitter.jar deployment deployment.xml

client: srccompile
	java -classpath obj:$(CLASSPATH) org.voltdb.twitter.drivers.Trends \
		$(TWITTER_USER) \ # twitter login username
		$(TWITTER_PASS) \ # twitter login password
		12345 \              # port to listen to for web server
		20 \                 # number of hashtags to show in each table
		localhost            # comma separated list of volt servers to connect to

export:
	echo todo

unique-user-count.jar: srccompile
	echo todo

hadoop-mr: srccompile
	echo todo