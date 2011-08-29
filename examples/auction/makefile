CLASSPATH = ../../obj/release/prod:../../lib
VOLTDB = ../../bin/voltdb
VOLTCOMPILER = ../../bin/voltcompiler
EXPORTTOFILE = ../../bin/exporttofile

.PHONY: main clean srccompile compile server client export

main: server

clean:
	rm -rf obj
	rm -rf debugoutput
	rm auction.jar
	rm -rf voltdbroot

srccompile:
	mkdir -p obj/com/auctionexample/datafiles
	cp src/com/auctionexample/datafiles/*.txt obj/com/auctionexample/datafiles/
	javac -classpath $(CLASSPATH) -d obj \
		src/com/auctionexample/*.java \
		procedures/com/auctionexample/*.java \
		procedures/com/auctionexample/debug/*.java

catalog: srccompile
	$(VOLTCOMPILER) obj project.xml auction.jar

server: catalog
	$(VOLTDB) create catalog auction.jar deployment deployment.xml

client: srccompile
	java -classpath obj:$(CLASSPATH) com.auctionexample.Client

export:
	$(EXPORTTOFILE) -classpath $(CLASSPATH) \
		--connect admin --servers localhost --type csv \
		--nonce EXPORTDEMO --user voltdb --password demo