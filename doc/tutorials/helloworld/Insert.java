import org.voltdb.*;

public class Insert extends VoltProcedure {

  public final SQLStmt sql = new SQLStmt(
      "INSERT INTO HELLOWORLD VALUES (?, ?, ?);"
  );

  public VoltTable[] run( String language,
                          String hello,
                          String world)
      throws VoltAbortException {
          voltQueueSQL( sql, hello, world, language );
          voltExecuteSQL();
          return null;
      }
}
