package signal;

import java.io.IOException;
import java.util.*;
import procedures.*;
import org.voltdb.*;
import org.voltdb.client.ClientFactory;


public class Client {

    static Integer num_of_satellites = 1;
    static String country = "usa";
    static double minlat =10.0;
    static double maxlat = 80.0;
    static double minoffset = -180.0;
    static double maxoffset = 180;
    static ArrayList<Orbit> sat = new ArrayList<Orbit>(20);
    final static String validcountries = "/usa/france/brazil/china/india/";
    final static org.voltdb.client.Client db = ClientFactory.createClient();

    /**
     * @param args
     */
    public static void main(String[] args) {
        /**
        * This is the main client for pushing satellite data into the database.
        * Based on the command line arguments, it will simulate a set of satellites
        * for a particular country. The allowable arguments are as follows:
        *
        * arg1 = (integer) number of satellites to generate.
        * arg2 = (string) the country that "owns" the satellites. Allowable values are
        *        usa, france, china, brazil, india
        *
        * The default values are 1 satellite from the USA.
        **/
        parsecommandline(args);

        // Initialize the database.
        init_db();

        // Generate the satellites.
        for (int i=0; i<num_of_satellites; i++) {
            Orbit o = new Orbit(Math.toRadians(minlat),Math.toRadians(maxlat),Math.toRadians(minoffset),Math.toRadians(maxoffset));
            o.country = country;
            o.id = i;
            o.model = country;
            sat.add(o);
            //Add to the database
            try {
                VoltTable[] result = db.callProcedure(AddSatellite.class.getSimpleName(),
                            o.id, o.model, o.country, o.currentLat, o.currentLong);
                o.id = (int) result[0].asScalarLong();
                System.out.println("Satellite ID is " + o.id);
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        /**
         * Move the satellites and send data to the database,
         * pausing momentarily between each move.
         */

        int counter = 0;
        while(counter < 3000) {        // Run for approximately 5 minutes
            counter++;
            for (Orbit s: sat) {
                s.Move();
                try {
                    VoltTable[] result = db.callProcedure(UpdateLocation.class.getSimpleName(),
                            s.id,  s.currentLat, s.currentLong);
                    assert (result != null);
                    long updatedRows = result[0].fetchRow(0).getLong(0);
                    assert(updatedRows > 0);
                }
                catch (Exception e) {
                    System.err.println(e.getMessage());
                }
            }
            pause(.1);
        }
        try {
            db.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void parsecommandline(String[] args) {
        if (args.length > 0) {
            try {
                    num_of_satellites = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    System.err.println("Usage: java signal.Client [num of satellites] [country]\n" +
                            "    Error: First argument must be an integer");
                    System.exit(1);
                }
            }
        if (num_of_satellites < 1) {
            System.err.println("Usage: java signal.Client [num of satellites] [country]\n" +
                            "    Error: First argument must be greater than zero.");
            System.exit(1);

        }

        if (args.length > 1) {
            country = args[1];
            country = country.toLowerCase();
            if(validcountries.contains("/" + country + "/") == false ) {
                System.err.println("Usage: java signal.Client [num of satellites] [country]\n" +
                        "    Error: Second argument is not a valid country name. \n" +
                        "            Must be one of the following: " + validcountries);
                System.exit(1);
            }
        }
            // Set some parameters so satellites always pass over their home country
        if (country.equals("usa"))    { minlat = 30;   maxlat = 45;     minoffset = -120;   maxoffset = -75; }
        if (country.equals("france")) { minlat = 42.5; maxlat = 51;    minoffset = -4;  maxoffset = 8; }
        if (country.equals("china"))  { minlat = 20; maxlat = 45;    minoffset = 75;  maxoffset = 130; }
        if (country.equals("brazil")) { minlat = -30; maxlat = -5;    minoffset = -70;  maxoffset = -35; }
        if (country.equals("india"))  { minlat = 8; maxlat = 32;    minoffset = 70;  maxoffset = 90; }
    }

    private static void pause(double secs) {
        try {
            Thread.sleep((long) (secs*1000) );
        }
        catch (Exception e) {
            System.out.print(e.getMessage());
            System.exit(1);
        }
    }

    static void init_db() {
        boolean started = false;
        while (!started) {
            try {
                db.createConnection("localhost", "signal", "wernher");
                started = true;
            }
            catch (java.net.ConnectException e) {
                System.out.print("Server not running yet. Pausing before Trying again...\n");
            }
            catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            if (!started) {
                pause(10.0);
            }
        }
    }
}
