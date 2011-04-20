hit de-duper application


additional documentation is available on the wiki at http://wiki.voltdb.com/display/internal/GhettoConfiguration 

to run this you'll need to adjust deployment.xml (# of servers and leader) and build.xml (volt-server-list property is the list of servers).

ant targets
-----------

catalog         : build the catalog

clean           : remove all compiled files

main            : compile all Java, build the catalog

server          : start a server

client-delete   : start a deleter client, delete old rows (archived rows older than start of day today)
                :   client runs until it no longer has rows to delete

client-export   : start the export client (pulls rows moved by client-mover-el)

client-mover    : start a mover client, move records from unarchived to archived, write to file
                :   *** moved rows are written to <milliseconds>_rows.txt in current directory

client-mover-el : start a mover client, move records from unarchived to archived and puts into export table

client-random   : start a data generator client, create random data

