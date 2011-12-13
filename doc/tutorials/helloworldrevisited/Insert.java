import org.voltdb.*;

@ProcInfo(
    singlePartition = false
)

public class Insert extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "INSERT INTO HELLOWORLD VALUES (?, ?, ?);"
  );

  public VoltTable[] run( String hello,
                          String world,
                          String language)
      throws VoltAbortException {
          voltQueueSQL( sql, hello, world, language );
          voltExecuteSQL();
          return null;
      }
}
