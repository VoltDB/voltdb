import org.voltdb.*;

@ProcInfo(
    partitionInfo = "GAME.game_id: 0",
    singlePartition = true
)

public class CountStarSmaller extends VoltProcedure {

  public final SQLStmt sql0 = new SQLStmt( "SELECT COUNT(*) FROM GAME WHERE game_id = ? and score < ?");

  public VoltTable[] run( long game_id, long scoreMin)
      throws VoltAbortException {
          voltQueueSQL( sql0, game_id, scoreMin );
          return voltExecuteSQL();
      }
}