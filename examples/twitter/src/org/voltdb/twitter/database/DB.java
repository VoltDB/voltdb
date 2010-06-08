package org.voltdb.twitter.database;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.twitter.database.procedures.Delete;
import org.voltdb.twitter.database.procedures.Insert;
import org.voltdb.twitter.database.procedures.Select;
import org.voltdb.twitter.util.HashTag;

public class DB {
    
    private Client client;
    
    public DB(List<String> servers) {
        client = ClientFactory.createClient();
        for (String server : servers) {
            try {
                client.createConnection(server, "program", "none");
            } catch (UnknownHostException e) {
                e.printStackTrace();
                System.exit(-1);
            } catch (IOException e) {
                System.err.println("Could not connect to database, terminating: (" + server + ")");
                System.exit(-1);
            }
        }
    }
    
    // insert a new hashtag
    public void insertHashTag(String hashTag) {
        try {
            client.callProcedure(
                    new ProcedureCallback() {

                        @Override
                        public void clientCallback(ClientResponse response) {}
                        
                    },
                    Insert.class.getSimpleName(),
                    hashTag,
                    System.currentTimeMillis() * 1000L);
        } catch (NoConnectionsException e) {
            System.err.println("Lost connection to database, terminating");
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // select hashtags within a certain time range
    public List<HashTag> selectHashTags(long maxAgeMicros, int limit) {
        try {
            // send the query to each partition and save the results
            List<HashTag> hashTags = new LinkedList<HashTag>();
            int partitions = getPartitions();
            for (int partition = 0; partition < partitions; partition++) {
                // execute the query on one of the partitions
                VoltTable[] tables = client.callProcedure(Select.class.getSimpleName(), partition,
                        System.currentTimeMillis() * 1000L - maxAgeMicros, limit).getResults();
                
                // nothing more to do if an empty result set was returned
                if (tables.length == 0) {
                    continue;
                }
                
                // add this partition's rows to the hashtag list
                VoltTable table = tables[0];
                VoltTableRow row = table.fetchRow(0);
                int rowCount = table.getRowCount();
                for (int i = 0; i < rowCount; i++) {
                    hashTags.add(new HashTag(row.getString(0), (int) row.getLong(1)));
                    row.advanceRow();
                }
            }
            
            // sort hashtags by count
            Collections.sort(hashTags);
            
            // apply the limit
            while (hashTags.size() > limit) {
                hashTags.remove(limit);
            }
            
            return hashTags;
        } catch (NoConnectionsException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    // delete hashtags older than a certain max age
    public long deleteHashTags(long maxAgeMicros) {
        try {
            long deleteAllEarlierThan = System.currentTimeMillis() * 1000L - maxAgeMicros;
            long deleteCount = client.callProcedure(Delete.class.getSimpleName(),
                    deleteAllEarlierThan).getResults()[0].asScalarLong();
            return deleteCount;
        } catch (NoConnectionsException e) {
            System.err.println("Lost connection to database, terminating");
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
        return -1;
    }
    
    // get the number of partitions in the database
    private int getPartitions() {
        try {
            return (int) client.callProcedure("@Statistics", "partitioncount",
                    0L).getResults()[0].fetchRow(0).getLong(0);
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
        return 0;
    }
    
}