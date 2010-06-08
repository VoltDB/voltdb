package game.drivers;

import game.Game;
import java.util.*;

public class GameDriver
{
    private static int defaultNumRows = 100;
    private static int defaultNumCols = 100;
    private static int defaultInitPopul = 4000;
    private static int defaultDelayInMillis = 0;
    private static String defaultHost = "localhost";

    public static void main(String[] args)
    {
        int numRows;
        int numCols;
        int initPopul;

        try
        {
            if (Integer.parseInt(args[0]) < 1)
            {
                System.err.println("Nonpositive value entered for number of rows.");
                throw new Exception();
            }
            else
                numRows = Integer.parseInt(args[0]);
            if (Integer.parseInt(args[1]) < 1)
            {
                System.err.println("Nonpositive value entered for number of columns.");
                throw new Exception();
            }
            else
                numCols = Integer.parseInt(args[1]);
            if (Integer.parseInt(args[2]) < 0)
            {
                System.err.println("Negative value entered for initial population.");
                throw new Exception();
            }
            else if (Integer.parseInt(args[2]) > numRows * numCols)
            {
                System.err.println("Value entered for initial population exceeds number of cells on board.");
                throw new Exception();
            }
            else
                initPopul = Integer.parseInt(args[2]);
            System.err.print("Running game with values: ");
        }
        catch (Exception e)
        {
            numRows = defaultNumRows;
            numCols = defaultNumCols;
            initPopul = defaultInitPopul;
            System.err.print("Invalid arguments or number of arguments. Reverting to defaults: ");
        }
        System.err.println(numRows + ", " + numCols + ", " + initPopul + ".");

        int delayInMillis;
        try
        {
            delayInMillis = Integer.parseInt(args[3]);
        }
        catch (Exception e)
        {
            delayInMillis = defaultDelayInMillis;
            System.err.println("Invalid arguments or number of arguments. Reverting to default delay in milliseconds: " + defaultDelayInMillis);
        }

        String[] hosts;
        try
        {
            hosts = args[4].split("\\s*,\\s*");
        }
        catch (Exception e)
        {
            hosts = new String[] {defaultHost};
            System.err.println("Invalid arguments or number of arguments. Reverting to default host: " + defaultHost);
        }

        Game g = new Game(numRows, numCols, delayInMillis, hosts);
        g.runGame(initPopul);
    }
}