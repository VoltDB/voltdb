import org.voltdb.*;

@ProcInfo(
    partitionInfo = "GAME.game_id: 1",
    singlePartition = true
)

public class Insert extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "INSERT INTO GAME VALUES (?, ? ,?);"
  );

  public VoltTable[] run( long id,
                          long game_id,
                          long score)
      throws VoltAbortException {
          voltQueueSQL( sql,  id, game_id, score);
          voltExecuteSQL();
          return null;
      }
}
