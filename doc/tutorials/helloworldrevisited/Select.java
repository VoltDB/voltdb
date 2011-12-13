import org.voltdb.*;

@ProcInfo(
    singlePartition = false
)

public class Select extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "SELECT HELLO, WORLD FROM HELLOWORLD " +
      " WHERE DIALECT = ?;"
  );

  public VoltTable[] run( String language)
      throws VoltAbortException {
          voltQueueSQL( sql, language );
          return voltExecuteSQL();
      }
}
