import org.voltdb.*;
import org.voltdb.client.*;
import java.util.Random;

public class Client {

    static String[] firstname = {"Juan","John","Jean","Pierre","Peter","Pedro",
		"William","Willhem","Carlo", "Carlos","Carlita","Carmen"};
    static String[] lastname = {"Brown","White","Black","Smith","Jones",
			"Green", "Calderon", "Tanaka","Lee","Lewis","Lincoln"};
    static String[] language = {"English","French","Spanish","Danish","Italian"};
    static String[] emailservice = {"gmail","yahoo","msn","juno","aol" };

    static int maxaccountID =0;


    public static void main(String[] args) throws Exception {

        /*
         * The command line has a comma-separated list of servers.
         * If not, use localhost
         */
        String serverlist = "localhost";
        if (args.length > 0) { serverlist = args[0]; }
        String[] servers = serverlist.split(",");

        /*
         * Instantiate a client and connect to all servers
         */
        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        for (String server: servers) { 
            myApp.createConnection(server);
        }

        /*
         * Load the database.
         */
        try {
            myApp.callProcedure("Insert", language[0], "Hello",   "World");
            myApp.callProcedure("Insert", language[1], "Bonjour", "Monde");
            myApp.callProcedure("Insert", language[2], "Hola",    "Mundo");
            myApp.callProcedure("Insert", language[3], "Hej",     "Verden");
            myApp.callProcedure("Insert", language[4], "Ciao",    "Mondo");
        } catch (Exception e) {
               // Not to worry. Ignore constraint violations if we 
               // load this table more than once. 
        }

        Random seed = new Random();
	
        /*
         * Start by making sure there are at least 5 accounts
         */
        while (maxaccountID < 5) {
             String first = firstname[seed.nextInt(10)];
             String last = lastname[seed.nextInt(10)];
             String dialect = language[seed.nextInt(5)];
             String email = generateEmail(maxaccountID);
             myApp.callProcedure(new RegisterCallback(),"RegisterUser",
                                   email,first,last,dialect );
             maxaccountID++;
   	}
 
       /*
         * Emulate a busy system: 100 signins for every 1 new registration.
	 * Run for 5 minutes.
         */
	long countdowntimer = System.currentTimeMillis() + (60 * 1000 * 5); 
	while (countdowntimer > System.currentTimeMillis()) {

             for (int i=0; i<100; i++) {
                   //int id = seed.nextInt(maxaccountID);
                   String user = generateEmail(seed.nextInt(maxaccountID));
                   myApp.callProcedure(new SignInCallback(), "SignIn",
                                       user, System.currentTimeMillis());
	     }
	
            String first = firstname[seed.nextInt(10)];
            String last = lastname[seed.nextInt(10)];
            String dialect = language[seed.nextInt(5)];
            String email = generateEmail(maxaccountID);

            myApp.callProcedure(new RegisterCallback(),"RegisterUser",
                                email,first,last,dialect);
            maxaccountID++;
 
	}

    }

//****************************** CALLBACKS *******************************//
 
    static class SignInCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) {

		// Make sure the procedure succeeded.
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(response.getStatusString());
                return;
             }

           VoltTable results[] = response.getResults();
           VoltTable recordset = results[0];

           System.out.printf("%s, %s!\n",
                recordset.fetchRow(0).getString("Hello"),
                recordset.fetchRow(0).getString("Firstname") );

       }
    }
    static class RegisterCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) {

		// Make sure the procedure succeeded. If not
                // (for example, account already exists), 
                // report the error.
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(response.getStatusString());
              }

         }
    }

//****************************** UTILITY ROUTINE *******************************//
    static private String generateEmail(int id) {
        String address;
        int pseudorandom = id % 5;
        address = Integer.toString(id) + "@" + emailservice[pseudorandom] + ".com";
        return address;
    }
 
}
