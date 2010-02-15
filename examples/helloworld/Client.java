import org.voltdb.*;
import org.voltdb.client.ClientFactory;

public class Client {

   public static void main(String[] args) {

              /*
               * Instantiate a client and connect to the database.
               */
    org.voltdb.client.Client myApp = null;
    try {
            myApp = ClientFactory.createClient();
            myApp.createConnection("localhost", "program", "password");
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        };
               /*
               * Load the database.
               */
    VoltTable[] results = null;
    try {
       results = myApp.callProcedure("Insert","Hello",  "World", "English");
       results = myApp.callProcedure("Insert","Bonjour","Monde", "French");
       results = myApp.callProcedure("Insert","Hola",   "Mundo", "Spanish");
       results = myApp.callProcedure("Insert","Hej",    "Verden","Danish");
       results = myApp.callProcedure("Insert","Ciao",   "Mondo", "Italian");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        };
 
              /*
             * Retrieve the message.
             */
    try {
       results = myApp.callProcedure("Select", "Spanish");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        };

  if (results.length != 1) {
      System.out.printf("I can't say Hello in that language.");
      System.exit(-1);
       }
 
  VoltTable resultTable = results[0];
  VoltTableRow row = resultTable.fetchRow(0);
  System.out.printf("%s, %s!\n", 
                    row.getString("hello"),  
                    row.getString("world"));
  }
}
