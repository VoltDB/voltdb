Voter application
===========================

Application simulates a phone based election process.
  - Voters are allowed a limited number of votes, based on phone number.  This number is passed to the client application.
  - The number of contestants is between 1 and 12.
  - Multiple client applications can run simultaneously.


ant targets
-----------

ant                 : compile all Java clients and stored procedures, build the catalog

ant server          : start the server

ant client          : start the client, more than 1 client is permitted

ant catalog         : build the catalog

ant clean           : remove compiled files
    
