-- additional index for catalog switch
CREATE INDEX P_JMPINDEX ON partitioned (adhocjmp);
CREATE PROCEDURE droppedRead PARTITION on table forDroppedProcedure column p AS SELECT * from forDroppedProcedure where p = ?;

CREATE PROCEDURE droppedWrite PARTITION on table forDroppedProcedure column p AS DELETE from forDroppedProcedure where p = ?;
