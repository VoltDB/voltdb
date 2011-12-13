import org.voltdb.*;

@ProcInfo(
    partitionInfo = "USERACCOUNT.EMAIL: 0",
    singlePartition = true
)

public class RegisterUser extends VoltProcedure {

  public final SQLStmt insertuser = new SQLStmt(
      "INSERT INTO USERACCOUNT VALUES (?,?,?,?,?);"
  );

  public VoltTable[] run( String email, String firstname, 
                   String lastname, String language)
      throws VoltAbortException {

              // Insert a new record using the Email
          voltQueueSQL( insertuser, email, firstname, 
                        lastname, null, language);
          return voltExecuteSQL();

      }

}
