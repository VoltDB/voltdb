import org.voltdb.*;

@ProcInfo(
    partitionInfo = "GAME.game_id: 0",
    singlePartition = true
)

public class CountStarRange extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt( "SELECT COUNT(*) FROM GAME WHERE game_id = ? and score > ? and score <= ?");

  public VoltTable[] run( long game_id, long scoreMin, long scoreMax)
      throws VoltAbortException {
          voltQueueSQL( sql1, game_id, scoreMin, scoreMax );
          return voltExecuteSQL();
      }
}