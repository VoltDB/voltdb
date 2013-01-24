import org.voltdb.*;

public class RegisterUser extends VoltProcedure {

  public final SQLStmt insertuser = new SQLStmt(
      "INSERT INTO USERACCOUNT VALUES (?,?,?,?,?);"
  );

  public VoltTable[] run( String email, String firstname, 
                   String lastname, String language)
      throws VoltAbortException {

              // Insert a new record
          voltQueueSQL( insertuser, email, firstname, 
                        lastname, null, language);
          return voltExecuteSQL();

      }

}
