package ##package_prefix##;

import java.io.IOException;

// VoltTable is VoltDB's table representation.
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientConfig;

// Procedures are invoked by class name. Import them to
// allow access to the class name programmatically.
import ##package_prefix##.procedures.*;

public class Client {

    /** Example SELECT: select rows matching the specified id parameter.
     *
     * Call <code>##package_prefix##.procedures.Select</code> passing
     * the <code>id</code> as the only parameter. An array of tables
     * one table long will be returned.
     *
     * Use <code>VoltTable.getRowCount()</code> and
     * <code>VoltTable.fetchRow(int row)</code> to read the resulting
     * rows. Use <code>VoltTableRow.getLong(int column)</code> to read
     * a specific column.
     *
     * @param long id: the id parameter value.
     * @throws java.io.IOException on client communication error.
     * @throws org.voltdb.client.Client.ProcCallException on VoltDB server error.
     */
    private void doSelect(int id) {
        System.out.println("Select(" + id + ")");
        try {
            VoltTable[] result = client.callProcedure(Select.class.getSimpleName(), id).getResults();
            assert result.length == 1;
            final int rowCount = result[0].getRowCount();
            if (rowCount == 0) {
                System.out.println("Selected: 0 rows selected.");
            }
            for (int ii = 0; ii < rowCount; ii++) {
                final VoltTableRow row = result[0].fetchRow(0);
                final long ##project_name##_ID = row.getLong(0);
                final long ##project_name##_ITEM = row.getLong(1);
                System.out.println("Selected: ##project_name##_ID == " + ##project_name##_ID + " ##project_name##_ITEM == " + ##project_name##_ITEM);
            }

        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
        catch (org.voltdb.client.ProcCallException e) {
            System.out.println("Select(" + id + ") failed: " + e.getMessage());
        }
    }

    /** Example INSERT: insert an id, item pair as specified by the id
     * and item parameters.
     *
     * Call <code>##package_prefix##.procedures.Insert</code> passing
     * the id and item parameters.  Inserts return one table
     * containing exactly one row with exactly one integer column. The
     * sole value is the count of inserted rows.
     *
     * Use <code>VoltTable.getRowCount()</code> and
     * <code>VoltTable.fetchRow(int row)</code> to read the resulting
     * rows. Use <code>VoltTableRow.getLong(int column)</code> to read
     * a specific column.
     *
     * @param id: the id parameter value.
     * @param item: the item parameter value.
     * @throws java.io.IOException on client communication error.
     * @throws org.voltdb.client.Client.ProcCallException on VoltDB server error.
     */
    private void doInsert(int id, long item) {
        System.out.println("Insert(" + id + "," + item + ")");
        try {
            VoltTable[] result = client.callProcedure(Insert.class.getSimpleName(), id, item).getResults();
            assert (result != null);
            long insertedRows = result[0].fetchRow(0).getLong(0);
            System.out.println("INSERT(" + id + "," + item + ") successfully inserted " + insertedRows + " row(s)");
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
        catch (org.voltdb.client.ProcCallException e) {
            System.out.println("Insert(" + id + "," + item + ") failed: " + e.getMessage());
        }
    }

    /** Example error: attempt an invalid INSERT.
     *
     * Call <code>##package_prefix##.procedures.Insert</code> with an
     * invalid number of parameters.  This procedure invocation will
     * <code>throw org.voltdb.client.Client.ProcCallException</code>.
     *
     * @param id: the id parameter value.
     * @throws java.io.IOException on client communication error.
     * @throws org.voltdb.client.Client.ProcCallException on VoltDB server error, expected in this example.
     */
    private void tryInvalidInsert(int id) {
        System.out.println("Insert(" + id + ")");
        try {
            VoltTable[] result = client.callProcedure(Insert.class.getSimpleName(), id).getResults();
            assert (result != null);
            System.out.println("INVALID INSERT(" + id + ") successful");
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
        catch (org.voltdb.client.ProcCallException e) {
            System.out.println("Demonstrate a response with an error string: " + e.getMessage());
        }
    }

    /** Example DELETE: delete the row corresponding to the specified id.
     *
     *  Call <code>##package_prefix##.procedures.Delete</code> passing
     *  the id parameter. Deletes return one table containing exactly
     *  one row with exactly one integer column. The sole value is the
     *  count of deleted rows.
     *
     * @param id: the id parameter value.
     * @throws java.io.IOException on client communication error.
     * @throws org.voltdb.client.Client.ProcCallException on VoltDB server error, expected in this example.
     */
    private void doDelete(int id) {
        System.out.println("Delete (" + id + ")");
        try {
            VoltTable[] result = client.callProcedure(Delete.class.getSimpleName(), id).getResults();
            assert (result != null);
            long deletedRows = result[0].fetchRow(0).getLong(0);
            System.out.println("DELETE(" + id + ") successfully deleted " + deletedRows + " row(s)");

        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }
        catch (org.voltdb.client.ProcCallException e) {
            System.out.println("Delete(" + id + ") failed: " + e.getMessage());
        }

    }

    public static void main(String[] args) {
        System.out.println("Client started");

        // org.voltdb.client.Client.createConnection is a static method that returns
        // an org.voltdb.client.Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        final ClientConfig clientConfig = new ClientConfig("program", "none");
        final org.voltdb.client.Client voltclient =
            org.voltdb.client.ClientFactory.createClient(clientConfig);
        try {
            voltclient.createConnection("localhost");
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final Client client = new Client(voltclient);

        client.doInsert( 1, 2 );      // insert id=1, item=2
        client.doInsert( 3, 4 );      // insert id=3, item=4
        client.doInsert( 5, 6 );      // insert id=5, item=6
        client.doSelect(5);           // select row where id=5
        client.doSelect(6);           // select row where id=6
        client.doSelect(3);           // select row where id=3
        client.doDelete(5);           // delete row where id=5
        client.doSelect(5);           // select row where id=5
        client.tryInvalidInsert(10);  // demonstrate procedure error.
        System.out.println("Client finished");
    }

    private final org.voltdb.client.Client client;

    private Client(org.voltdb.client.Client client) {
        this.client = client;
    }
}
