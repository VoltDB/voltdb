package org.voltdb.benchmark.workloads;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.client.Client;

public class RandomLoader// implements Loader
{
    private static class GenericCallback implements ProcedureCallback
    {
        public void clientCallback(ClientResponse clientResponse)
        {
            //do error checking
            //param type matching
            //invalid inserts (primary key)
            //invalid deletes/selects (no such rows)
        }
    }

    public static void run(Client client)
    {
        int numInserts = Short.MAX_VALUE;
        try
        {
            String randString;
            //int randInt;
            for (int i = 0; i < numInserts; i++)
            {
                randString = RandomValues.getString(100, true);
                //randInt = RandomValues.getInt();
                client.callProcedure(new GenericCallback(), "Insert", (int)(Math.random() * numInserts), randString);
            }
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        catch (ConnectException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}