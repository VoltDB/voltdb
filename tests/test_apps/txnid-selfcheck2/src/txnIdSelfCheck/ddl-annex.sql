-- additional index for catalog switch
CREATE INDEX P_JMPINDEX ON partitioned (adhocjmp);
CREATE PROCEDURE droppedRead AS SELECT * from forDroppedProcedure where p = ?;
PARTITION PROCEDURE droppedRead on table forDroppedProcedure column p;
CREATE PROCEDURE droppedWrite AS DELETE from forDroppedProcedure where p = ?;
PARTITION PROCEDURE droppedWrite on table forDroppedProcedure column p;
