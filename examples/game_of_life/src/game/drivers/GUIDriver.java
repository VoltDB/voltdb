package game.drivers;

import game.db.*;
import game.gui.*;

public class GUIDriver
{
    private static int defaultDelayInMillis = 500;
    private static String defaultHost = "localhost";

    public static void main(String args[])
    {
        String[] hosts;
        try
        {
            hosts = args[1].split("\\s*,\\s*");
        }
        catch (Exception e)
        {
            hosts = new String[] {defaultHost};
            System.err.println("Invalid arguments or number of arguments. Reverting to default host: " + defaultHost);
        }

        DB db = new DB("GUI", hosts);
        int[] rcArray = db.getRowCol();

        int delayInMillis;
        try
        {
            delayInMillis = Integer.parseInt(args[0]);
        }
        catch (Exception e)
        {
            delayInMillis = defaultDelayInMillis;
            System.err.println("Invalid arguments or number of arguments. Reverting to default delay in milliseconds: " + defaultDelayInMillis);
        }

        GUI gui = new GUI(db, rcArray[0], rcArray[1], 5, delayInMillis);

        boolean repeat = true;
        while (repeat)
        {
            gui.repaint();
            try
            {
                Thread.sleep(gui.DELAY_MILLIS);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}