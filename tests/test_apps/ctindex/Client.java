import org.voltdb.*;
import org.voltdb.client.*;

public class Client {

    public static void main(String[] args) throws Exception {

        /*
         * Instantiate a client and connect to the database.
         */
        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        myApp.createConnection("localhost");

        long start;
        start = System.currentTimeMillis();
        /*
         * Load the database.
         */
        final long rows = 1000000;
        final long print = 10000;
        long partitions = 2;
        
        for (long i = 0; i < rows; i++) {
            myApp.callProcedure("Insert", i, i % partitions, i);
            if (i % print == 0)
                System.out.println("Insert " + i + " rows");
        }
        double latency = (System.currentTimeMillis() - start) / 1000F;
        System.out.println(String.format("Loading %d rows data elaspsed: %.3f seconds", rows,  latency));
        /*
         * Retrieve the message.
         
        ClientResponse response;
        start =  System.currentTimeMillis();
        final long csl = rows;
        final long csr = rows;
        for (long i = 0; i < csl; i++) {
            response = myApp.callProcedure("CountStarSmaller", i % partitions, i );
            if (response.getStatus() != ClientResponse.SUCCESS){
                System.err.println(response.getStatusString());
                System.exit(-1);
            }
            if (i % print == 0)
                System.out.println("Execute CountStarSmaller " + i + " times");
        }
        double latency1 = (System.currentTimeMillis() - start) / 1000F;
        System.out.println(String.format("CountStarSmaller query execute %d times, elaspsed: %.3f seconds, throughput: %.3f txns/s", 
                csl, latency1 , csl / latency1 ));
        
        start = System.currentTimeMillis();
        for (long i = 0; i < csr; i++) {
            response =myApp.callProcedure("CountStarRange", i % partitions, i, i + (long)((rows - i) / 2));
            if (response.getStatus() != ClientResponse.SUCCESS){
                System.err.println(response.getStatusString());
                System.exit(-1);
            }
            if (i % print == 0)
                System.out.println("Execute CountStarRange " + i + " times");
        }
        double latency2 = (System.currentTimeMillis() - start) / 1000F;
        System.out.println(String.format("CountStarRange query execute %d times, elaspsed: %.3f seconds, throughput: %.3f txns/s", 
                csr, latency2 , csr / latency2));
        
        
        System.out.println(String.format("Total queries execute %d times, elaspsed: %.3f seconds, throughput: %.3f txns/s", 
                (csr + csl) ,(latency1 + latency2) , (csr + csl)/ ( latency1 + latency2) ));
                
         */

    }
}
