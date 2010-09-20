/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package game.db;

import game.procedures.GetGeneration;
import game.procedures.GetIDs;
import game.procedures.GetRowCol;
import game.procedures.IsOccupied;
import game.procedures.Occupy;
import game.procedures.SetIDs;
import game.procedures.SetRowCol;
import game.procedures.UpdateGeneration;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class DB
{
    //unused callback
    static class GenericCallback implements ProcedureCallback
    {
        public void clientCallback(ClientResponse clientResponse)
        {
            //do error checking
        }
    }

    private final Client client;

    //creates new client
    public DB(String clientName, String[] hosts)
    {
        System.out.println(clientName + " client started");
        ClientConfig config = new ClientConfig("program", "none");
        client = ClientFactory.createClient(config);
        connect(hosts);
    }

    //initializes cell's status as live or dead in database
    public void setIDs(int id, int occupy)
    {
        try
        {
            client.callProcedure(new GenericCallback(), SetIDs.class.getSimpleName(), id, occupy);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    //returns statuses of cells as live (true) or dead (false) from database
    public boolean[] getIDs()
    {
        try
        {
            VoltTable results = client.callProcedure(GetIDs.class.getSimpleName()).getResults()[0];
            boolean[] ids = new boolean[results.getRowCount()];
            for (int i = 0; i < ids.length; i++)
                if (results.fetchRow(i).getLong(1) == 0)
                    ids[(int)results.fetchRow(i).getLong(0)] = false;
                else
                    ids[(int)results.fetchRow(i).getLong(0)] = true;
            return ids;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (ProcCallException e)
        {
            System.out.println("Getting ids failed: " + e.getMessage());
        }
        return null;
    }

    //sets cell's status in database as live if occupy is true and as dead if occupy is false
    public void occupy(boolean occupy, int id)
    {
        try
        {
            int to_occupy = 0;
            if (occupy)
                to_occupy = 1;
            client.callProcedure(new GenericCallback(), Occupy.class.getSimpleName(), to_occupy, id);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    //returns current generation number from database
    public long getGeneration()
    {
        try
        {
            int defaultPK = 0;
            return client.callProcedure(GetGeneration.class.getSimpleName(), defaultPK).getResults()[0].fetchRow(0).getLong(0);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (ProcCallException e)
        {
            System.out.println("Getting generation failed: " + e.getMessage());
        }
        return 0;
    }

    //adds 1 to current generation number in database
    public void updateGeneration()
    {
        try
        {
            int defaultPK = 0;
            client.callProcedure(new GenericCallback(), UpdateGeneration.class.getSimpleName(), defaultPK);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    //sets the number of rows and columns of the board in database
    public void setRowCol(int numRows, int numCols)
    {
        try
        {
            int defaultPK = 0;
            int generation = 0;
            client.callProcedure(new GenericCallback(), SetRowCol.class.getSimpleName(), defaultPK, numRows, numCols, generation);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    //returns the number of rows and columns of the board from database
    public int[] getRowCol()
    {
        try
        {
            int defaultPK = 0;
            VoltTableRow results = client.callProcedure(GetRowCol.class.getSimpleName(), defaultPK).getResults()[0].fetchRow(0);
            int[] rcArray = new int[2];
            rcArray[0] = (int)results.getLong(0);
            rcArray[1] = (int)results.getLong(1);
            return rcArray;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (ProcCallException e)
        {
            System.out.println("Getting numRows and numCols failed: " + e.getMessage());
        }
        return null;
    }

    //checks if cell is live or dead
    public boolean isOccupied(int id)
    {
        try
        {
            if (client.callProcedure(IsOccupied.class.getSimpleName(), id).getResults()[0].fetchRow(0).getLong(0) == 0)
                return false;
            else
                return true;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (ProcCallException e)
        {
            System.out.println("Checking if occupied failed: " + e.getMessage());
        }
        return false;
    }

    //creates client connection to database
    public void connect(String[] hosts)
    {
        try
        {
            for (String host : hosts)
                if (!host.equals(""))
                    client.createConnection(host);
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

    //disconnects client from database
    public void disconnect()
    {
        try
        {
            client.drain();
            client.close();
            System.out.println("Client finished");
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (NoConnectionsException e)
        {
            System.out.println("No connection exception: " + e.getMessage());
        }
    }
}