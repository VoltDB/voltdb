package benchmark;

import org.voltdb.*;
import org.voltdb.client.*;

public class SeedTables {

    public static void main(String[] args) throws Exception {

        /*
         * Instantiate a client and connect to the database.
         */
        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        myApp.createConnection("localhost");


        VoltTable p = myApp.callProcedure("@GetPartitionKeys","INTEGER").getResults()[0];
        int i = 0;
        while (p.advanceRow()) {
            i++;
            long id = p.getLong(0);
            myApp.callProcedure("TMP_0.insert",0,id,0);
            myApp.callProcedure("TMP_1.insert",0,id,0);
            myApp.callProcedure("TMP_2.insert",0,id,0);
            myApp.callProcedure("TMP_3.insert",0,id,0);
            myApp.callProcedure("TMP_4.insert",0,id,0);
            myApp.callProcedure("TMP_5.insert",0,id,0);
            myApp.callProcedure("TMP_6.insert",0,id,0);
            myApp.callProcedure("TMP_7.insert",0,id,0);
            myApp.callProcedure("TMP_8.insert",0,id,0);
            myApp.callProcedure("TMP_9.insert",0,id,0);

            myApp.callProcedure("TMP_s0.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s1.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s2.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s3.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s4.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s5.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s6.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s7.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s8.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s9.insert",0,id,0,"FOO");

        }
        System.out.println("Finished seeding " + i + " partitions.");
    }
}
