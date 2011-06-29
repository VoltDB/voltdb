# Aggregates test application

This application loads predictable data and executes distributed aggregates and
limits against that data set. This allows us to test multi-node pushdown plans
that fail if pushdown is not correct.

## Schema

P1: Partitioned on P1PK. 
    INT1 is (P1PK * -1). 
    VARCHAR1 is 1000 bytes to force large results.

P2: Partitioned on P1FK. There are (P1PK % 5) rows in P2 for each P1PK. 
    P2PK is unique.
    INT1 is P2PK * -1
    VARCHAR1 is 1000 bytes to force large results
 
This allows us to join multiple P2 rows against P1 on the P1PK = P1FK.
 
