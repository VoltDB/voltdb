package org.voltdb.client.VoltBulkLoader;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterConverter;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoaderRow.BulkLoaderNotification;

/**
 * VoltBulkLoader is meant to run for long periods of time. Multiple threads can
 * operate on a single instance of the VoltBulkLoader to feed and bulk load a
 * single table. It is also possible for multiple instances of the
 * VoltBulkLoader to operate concurrently no the same table or different tables
 * as long as they share the same Client instance.
 * 
 * All instances of VoltBulkLoader using a common Client share a pool of threads
 * dedicated to feeding individual partitions. There is also a thread dedicated
 * to processing multi-partition tables. Finally, each instance of
 * VoltBulkLoader has a thread, dedicated to processing failed bulk inserts. If
 * multiple threads are inserting rows into the same VoltBulkLoader instance and
 * batch inserts are relatively frequent, using multiple VoltBulkLoaders should
 * improve performance characteristics particularly if one thread is injecting
 * more failed rows than other threads. If one thread is injecting too many
 * failures, relative to other threads operating on the same table, that thread
 * should be isolated a different process. If failed rows are a common problem,
 * throughput can also be improved by using multiple instances of VoltBulkLoader.
 */
public class VoltBulkLoader {
	final VoltBulkLoaderGlobals m_vblGlobals;
	final ClientImpl m_clientImpl;
	// Batch size requested for this instance of VoltBulkLoader
	final int m_maxBatchSize;
	// Callback used to notify users of failed row inserts
	final BulkLoaderFailureCallBack m_notificationCallBack;
    //Array of PerPartitionTables from which this VoltBulkLoader chooses to put a row in
    PerPartitionTable[] m_partitionTable = null;
    //Index in m_partitionTable of first partition
    final int m_firstPartitionTable;
    //Index in m_partitionTable of last partition
    final int m_lastPartitionTable;

    // Name of procedure used by this instance of the VoltBulkLoader
    final String m_procName;
    //Table name to insert into.
    String m_tableName;
    //Type of partitioned column
    VoltType m_partitionColumnType = VoltType.NULL;
    //Column information
    VoltTable.ColumnInfo m_colInfo[];
    //Column types
    //Map<Integer, VoltType> m_columnTypes;
    VoltType[] m_columnTypes;
    //Index of partitioned column in table
    int m_partitionedColumnIndex = -1;
    //Column Names
    Map<Integer, String> m_colNames;
    //Number of columns
    int m_columnCnt = 0;
    //Is this a MP transaction
    private boolean m_isMP = false;
    //Total number of partition processors including the MP processor.
    private int m_maxPartitionProcessors = -1;

    // Dedicated thread for processing all failed batch rows inserted by this VoltBulkLoader instance
    FailedBatchProcessor m_failureProcessor = null;
    //Queue of batch entries where some rows failed.
    BlockingQueue<VoltBulkLoaderRow> m_failedQueue = null;

    //Number of rows inserted into the PerPartitionTable queues.
    final AtomicLong m_loaderQueuedRowCnt = new AtomicLong(0);
    //Number of rows removed from PerPartitionTable queue converted to VoltTable and submitted to Client
    final AtomicLong m_loaderBatchedRowCnt = new AtomicLong(0);
    //Number of rows for which we have received a definitive success or failure.
    final AtomicLong m_loaderCompletedCnt = new AtomicLong(0);
    //Number of rows waiting to be sent by the BatchFailureProcessor 
    final AtomicLong m_failedBatchQueuedRowCnt = new AtomicLong(0);
    //Number of rows sent by the BatchFailureProcessor
    final AtomicLong m_failedBatchSentRowCnt = new AtomicLong(0);

    // Object maintains the running row count for each VoltBulkLoader in a given batch
    static class LoaderPair {
        VoltBulkLoader loader;
        int rowCnt;
        public LoaderPair(VoltBulkLoader l, int c){
            this.loader = l;
            this.rowCnt = c;
        }
    }

    // Permanent set of all outstanding batches organized by partitionId
    final ArrayList<LoaderPair>[] m_outstandingRowCnts;
    // Stack of entries in outstandingRowCnts that are not being used organized by partitionId
    final Stack m_availLoaderPairs;
    // LoaderPair currently being built by a PerPartitionTable organized by partitionId
    final LoaderPair[] m_currBatchPair;
    

    //This is the thread that processes one insert at a time if the batch fails
    private class FailedBatchProcessor extends Thread {
        CountDownLatch m_failedBatchProcessor_cdl = null;

        //Callback for single row procedure invoke called for rows in failed batch.
        class PartitionFailureExecuteProcedureCallback implements ProcedureCallback {
            final VoltBulkLoaderRow m_row;

            public PartitionFailureExecuteProcedureCallback(VoltBulkLoaderRow row) {
                m_row = row;
            }

            @Override
            public void clientCallback(ClientResponse response) throws Exception {
                //one insert at a time callback
                if (response.getStatus() != ClientResponse.SUCCESS) {
                	m_notificationCallBack.failureCallback(m_row.rowHandle, m_row.objectList, response);
                    m_log.error(response.getStatusString());
                }
                m_loaderCompletedCnt.incrementAndGet();
                if (m_failedBatchSentRowCnt.decrementAndGet() == 0) {
                	if (m_failedBatchProcessor_cdl != null) {
                		m_failedBatchProcessor_cdl.countDown();
                		m_failedBatchProcessor_cdl = null;
                	}
                }
            }
        }
    	
        @Override
        public void run() {
            while (true) {
                try {
                	VoltBulkLoaderRow currRow;
                    currRow = m_failedQueue.take();
                    //If we see a syncRow process the special condition.
                    if (currRow.isNotificationRow()) {
                        m_log.debug("notificationRow for table " + m_tableName + " received by failure process");
                        assert(m_failedBatchProcessor_cdl == null);
            			BulkLoaderNotification notifier = ((BulkLoaderNotification)currRow.rowHandle);
            			m_failedBatchProcessor_cdl = notifier.getLatch();
            			if (m_failedBatchSentRowCnt.get() == 0) {
                    		m_failedBatchProcessor_cdl.countDown();
                    		m_failedBatchProcessor_cdl = null;
            			}
            			if (notifier instanceof VoltBulkLoaderRow.CloseNotificationCallBack)
            				break;
                        continue;
                    }
                    m_failedBatchQueuedRowCnt.decrementAndGet();
                    m_failedBatchSentRowCnt.incrementAndGet();
                    try {
                        VoltTable table = new VoltTable(m_colInfo);
                        //No need to check error here if a correctedLine has come here it was previously successful.
                        try {
                	        Object row_args[] = new Object[currRow.objectList.length];
                	        for (int i = 0; i < row_args.length; i++) {
                	            final VoltType type = m_columnTypes[i];
                	            row_args[i] = ParameterConverter.tryToMakeCompatible(type.classFromType(), currRow.objectList[i]);
                	        }
                	        table.addRow(row_args);
                        } catch (VoltTypeException ex) {
                        	// Should never happened because the bulk conversion in PerPartitionProcessor should have caught this 
                        	continue;
                        }

                        PartitionFailureExecuteProcedureCallback callback =
                                new PartitionFailureExecuteProcedureCallback(currRow);
                        if (!m_isMP) {
                            Object rpartitionParam =
                                    TheHashinator.valueToBytes(table.fetchRow(0).get(m_partitionedColumnIndex, m_partitionColumnType));
                            m_clientImpl.callProcedure(callback, m_procName, rpartitionParam, m_tableName, table);
                        }
                        else
                        	m_clientImpl.callProcedure(callback, m_procName, m_tableName, table);
                    } catch (IOException ioex) {
                        //Put this processor in error so we exit
                        m_log.warn("Fallback to single row inserts failed, failures will not be processed: " + ioex);
                        m_failedQueue.clear();
                        m_failedQueue = null;
                        break;
                    } 
                } catch (InterruptedException ex) {
                    //Put this processor in error so we exit
                    m_log.debug("Stopped failure processor.");
                    m_failedQueue.clear();
                    m_failedQueue = null;
                    break;
                }
            }
            m_log.debug("Done Processing failed bulk rows for table: " + m_tableName);
        }
    }

    
    private static final VoltLogger m_log = new VoltLogger("BULKLOADER");
    static {
    	// Initialize logger in Partition Processor
    	PartitionProcessor.initializeLogger(m_log);
    	PerPartitionTable.initializeLogger(m_log);
    }

    // Constructor allocated through the Client to ensure consistency of VoltBulkLoaderGlobals
    public VoltBulkLoader(VoltBulkLoaderGlobals vblGlobals, String tableName, int maxBatchSize,
				BulkLoaderFailureCallBack blfcb) throws Exception {
		this.m_clientImpl = vblGlobals.m_clientImpl;
		this.m_maxBatchSize = maxBatchSize;
		this.m_notificationCallBack = blfcb;

		m_vblGlobals = vblGlobals;

        // Get table details and then build partition and column information.
		// Table analysis could be done once per unique table name but then the
		// m_TableNameToLoader lock would have to be held for a much longer.
		m_tableName = tableName;
        VoltTable procInfo = m_clientImpl.callProcedure("@SystemCatalog",
                "COLUMNS").getResults()[0];
        
        TreeMap<Integer, VoltType> columnTypes = new TreeMap<Integer, VoltType>();
        m_colNames = new TreeMap<Integer, String>();
        m_partitionedColumnIndex = -1;
        m_partitionColumnType = VoltType.NULL;
        
        while (procInfo.advanceRow()) {
            String table = procInfo.getString("TABLE_NAME");
            if (tableName.equalsIgnoreCase(table)) {
                VoltType vtype = VoltType.typeFromString(procInfo.getString("TYPE_NAME"));
                int idx = (int) procInfo.getLong("ORDINAL_POSITION") - 1;
                columnTypes.put(idx, vtype);
                m_colNames.put(idx, procInfo.getString("COLUMN_NAME"));
                String remarks = procInfo.getString("REMARKS");
                if (remarks != null && remarks.equalsIgnoreCase("PARTITION_COLUMN")) {
                	m_partitionColumnType = vtype;
                	m_partitionedColumnIndex = idx;
                    m_log.debug("Table " + tableName + " Partition Column Name is: "
                            + procInfo.getString("COLUMN_NAME"));
                    m_log.debug("Table " + tableName + " Partition Column Type is: " + vtype.toString());
                }
            }
        }
        m_columnCnt = columnTypes.size();

        if (m_columnCnt == 0) {
            //VoltBulkLoader will exit.
            m_log.error("Table " + tableName + " Not found");
            throw new Exception("Table Name parameter does not match any known table.");
        }
        m_columnTypes = (VoltType[])columnTypes.values().toArray(new VoltType[columnTypes.size()]);
        
        //Build column info so we can build VoltTable
        m_colInfo = new VoltTable.ColumnInfo[m_columnCnt];
        for (int i = 0; i < m_columnCnt; i++) {
            VoltType type = m_columnTypes[i];
            String cname = m_colNames.get(i);
            VoltTable.ColumnInfo ci = new VoltTable.ColumnInfo(cname, type);
            m_colInfo[i] = ci;
        }
        
        int sitesPerHost = 1;
        int kfactor = 0;
        int hostcount = 1;
        procInfo = m_clientImpl.callProcedure("@SystemInformation",
                "deployment").getResults()[0];
        while (procInfo.advanceRow()) {
            String prop = procInfo.getString("PROPERTY");
            if (prop != null && prop.equalsIgnoreCase("sitesperhost")) {
                sitesPerHost = Integer.parseInt(procInfo.getString("VALUE"));
            }
            if (prop != null && prop.equalsIgnoreCase("hostcount")) {
                hostcount = Integer.parseInt(procInfo.getString("VALUE"));
            }
            if (prop != null && prop.equalsIgnoreCase("kfactor")) {
                kfactor = Integer.parseInt(procInfo.getString("VALUE"));
            }
        }

        m_isMP = (m_partitionedColumnIndex == -1 ? true : false);
        // Dedicate a PartitionProcessor to MP tables
        m_maxPartitionProcessors = ((hostcount * sitesPerHost) / (kfactor + 1)) + 1;

        int queueDepthMultiplier;
        if (!m_isMP) {
        	m_firstPartitionTable = 0;
        	m_lastPartitionTable = m_maxPartitionProcessors-2;
            queueDepthMultiplier = Math.max(5, 1000/(m_maxPartitionProcessors-1));
            m_log.debug("Number of Partitions: " + (m_maxPartitionProcessors-1));
            m_log.info("Bulk Loader will attempt to load rows in batches of size: " + maxBatchSize);
        	m_procName = "@LoadSinglepartitionTable" ;
        }
        else {
        	queueDepthMultiplier = 1000;
        	m_firstPartitionTable = m_maxPartitionProcessors-1;
        	m_lastPartitionTable = m_maxPartitionProcessors-1;
           	m_procName = "@LoadMultipartitionTable" ;
       }
        
        synchronized (m_vblGlobals.m_TableNameToLoader) {
        	if (m_vblGlobals.m_TableNameToLoader.size()==0) {
        		// Set up the PartitionProcessors
        		m_vblGlobals.m_spawnedPartitionProcessors = new ArrayList<Thread>(m_maxPartitionProcessors);
        		m_vblGlobals.m_partitionProcessors = new PartitionProcessor[m_maxPartitionProcessors];
        		
        		m_vblGlobals.tableQueues = (ConcurrentLinkedQueue<PerPartitionTable>[]) new ConcurrentLinkedQueue[m_maxPartitionProcessors];
        		m_vblGlobals.m_processor_cdl = new CountDownLatch(m_maxPartitionProcessors);
        		for (int i=0; i<m_maxPartitionProcessors; i++) {
        			ConcurrentLinkedQueue<PerPartitionTable> tableQueue = new ConcurrentLinkedQueue<PerPartitionTable>();
        			m_vblGlobals.tableQueues[i] = tableQueue;
        			PartitionProcessor processor = new PartitionProcessor(i, i==m_maxPartitionProcessors-1, m_vblGlobals);
        			m_vblGlobals.m_partitionProcessors[i] = processor;
        			Thread th = new Thread(processor);
        			th.setName(processor.m_processorName);
        			m_vblGlobals.m_spawnedPartitionProcessors.add(th);
        			th.start();
        		}
        	}
        	
        	// Set up LoaderPair tables for managing multi-loader statistics
        	m_outstandingRowCnts = (ArrayList<LoaderPair>[]) Array.newInstance(ArrayList.class, m_maxPartitionProcessors);
    	    m_availLoaderPairs = new Stack();
    	    m_currBatchPair = new LoaderPair[m_maxPartitionProcessors];
    	    
        	List<VoltBulkLoader> loaderList = m_vblGlobals.m_TableNameToLoader.get(m_tableName);
        	if (loaderList == null) {
        		// First BulkLoader for this table
        		m_partitionTable = new PerPartitionTable[m_maxPartitionProcessors];
        		// Set up the BulkLoaderPerPartitionTables
        		for(int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++) {
            		m_partitionTable[i] = new PerPartitionTable(m_clientImpl, m_tableName, m_vblGlobals.m_partitionProcessors[i],
            				this, maxBatchSize, maxBatchSize*queueDepthMultiplier);
            		m_outstandingRowCnts[i] = new ArrayList<LoaderPair>();
        		}
        		loaderList = new ArrayList<VoltBulkLoader>();
        		loaderList.add(this);
        		m_vblGlobals.m_TableNameToLoader.put(m_tableName, loaderList);
        	}
        	else {
        		// Nth loader for this table
        		VoltBulkLoader primary = loaderList.get(0);
        		m_partitionTable = primary.m_partitionTable;
        		loaderList.add(this);
       			for(int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++) {
               		if (primary.m_maxBatchSize != maxBatchSize) {
        				m_partitionTable[i].updateMinBatchTriggerSize(maxBatchSize);
        			}
            		m_outstandingRowCnts[i] = new ArrayList<LoaderPair>();
        		}
        	}
        }
        
        //Launch failureProcessor
        m_failedQueue = new LinkedBlockingQueue<VoltBulkLoaderRow>();
        m_failureProcessor = new FailedBatchProcessor();
        m_failureProcessor.start();
	}

	void generateError(Object rowHandle, Object[] objectList, String errMessage) {
        VoltTable[] dummyTable = new VoltTable[1];
        dummyTable[0] = new VoltTable(m_colInfo);
		ClientResponse dummyResponse = new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE, dummyTable, errMessage);
		m_notificationCallBack.failureCallback(rowHandle, objectList, dummyResponse);
		m_loaderCompletedCnt.incrementAndGet();
	}
	
	/**
	 *  <p>Add new row to VoltBulkLoader table.</p>
	 * 
	 * @param caller supplied object used to distinguish failed insert attempts 
	 * @param list of fields associated with a single row
	 */
	public void insertRow(Object rowHandle, Object... fieldList) {
		int partitionId = 0;
        //Find partition to send this row to and put on correct PerPartitionTable.
		if (fieldList == null || fieldList.length <= 0) {
			String errMsg; 
			if (rowHandle == null)
				errMsg = "Error: insertRow received empty fieldList";
			else
				errMsg = "Error: insertRow received empty fieldList for row: " + rowHandle.toString();
			m_log.error(errMsg);
			generateError(rowHandle, fieldList, errMsg);
			return;
		}
        if (fieldList.length != m_columnCnt) {
        	String errMsg;
			if (rowHandle == null)
				errMsg = "Error: insertRow received incorrect number of columns; " + fieldList.length +
						" found, " + m_columnCnt + " expected";
			else
				errMsg = "Error: insertRow received incorrect number of columns; " + fieldList.length +
						" found, " + m_columnCnt + " expected for row: " + rowHandle.toString();
			m_log.error(errMsg);
			generateError(rowHandle, fieldList, errMsg);
			return;
        }
		VoltBulkLoaderRow newRow = new VoltBulkLoaderRow(this, rowHandle, fieldList);
        if (m_isMP) {
        	m_partitionTable[m_firstPartitionTable].insertRowInTable(newRow);
        }
        else {
            try {
				partitionId = (int)m_clientImpl.getPartitionForParameter(
						m_partitionColumnType.getValue(), fieldList[m_partitionedColumnIndex]);
				m_partitionTable[partitionId].insertRowInTable(newRow);
			} catch (VoltTypeException e) {
				m_log.error(e.getMessage());
				generateError(rowHandle, fieldList, e.getMessage());
				return;
			}
        }
        m_loaderQueuedRowCnt.incrementAndGet();
	}
	
	/**
	 * Called to asynchronously force the VoltBulkLoader to submit all the partially full batches
	 * in all partitions of the table to the Client for insert. To wait for all rows to be processed,
	 * use drain(). This method will also flush pending rows submitted by other VoltBulkLoader
	 * instances working on the same table and using the same instance of Client.
	 */
	public void flush() {
       	for (int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++)
       		m_partitionTable[i].flushAllTableQueues();
	}
	
	/**
	 * Removes all rows not already batched and submitted to the client to be removed. No
	 * callback notifications will be provided for aborted rows. This method will only remove
	 * rows inserted by this instance of VoltBulkLoader. rowInsert()s submitted on other
	 * instances of VoltBulkLoader (even those operating on the same table) will be unaffected. 
	 */
	public synchronized void abort() {
       	for (int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++)
       		m_partitionTable[i].abortFromLoader(this);
    	List<VoltBulkLoaderRow> allPartitionRows = new ArrayList<VoltBulkLoaderRow>();
    	m_failedQueue.drainTo(allPartitionRows);
    	m_failedBatchQueuedRowCnt.addAndGet(-1*allPartitionRows.size());
	}
	
	/**
	 * Called to synchronously force the VoltBulkLoader to submit all the partially full batches
	 * in all partitions of the table to the Client for insert. This call will wait until all
	 * previously submitted rows (including retried failed batch rows) have been processed and
	 * received responses from the Client.
	 */
	public synchronized void drain() {
		// Wait for number of PerPartitionTables we are using and the Failure Processor
		CountDownLatch waitLatch = new CountDownLatch(m_lastPartitionTable-m_firstPartitionTable+1);
       	for (int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++) {
    		VoltBulkLoaderRow drainRow = new VoltBulkLoaderRow(this);
    		drainRow.new DrainNotificationCallBack(waitLatch);
       		m_partitionTable[i].drainTableQueue(drainRow);
       	}
		try {
			waitLatch.await();
		} catch (InterruptedException e) {
		}
		assert(m_loaderBatchedRowCnt.get() == 0 && m_loaderQueuedRowCnt.get() == 0);

		// Now we know all batches have been processed so wait for all failedQueue messages to complete
		CountDownLatch batchFailureQueueLatch = new CountDownLatch(1);
		VoltBulkLoaderRow drainRow = new VoltBulkLoaderRow(this);
		drainRow.new DrainNotificationCallBack(batchFailureQueueLatch);
       	m_failedQueue.add(drainRow);
		try {
			batchFailureQueueLatch.await();
		} catch (InterruptedException e) {
		}
		assert(m_failedBatchQueuedRowCnt.get() == 0 && m_failedBatchSentRowCnt.get() == 0);
	}
	
	/**
	 * Waits for all pending inserts to be acknowledged and then closes this instance of the
	 * VoltBulkLoader. During and after the invocation of close(), calls to insertRow will get
	 * an Exception. All other instances of VoltBulkLoader will continue to function.
	 */
	public synchronized void close() {
    	PerPartitionTable tmpTable;
    	// Remove this VoltBulkLoader from the active set. 
        synchronized (m_vblGlobals.m_TableNameToLoader) {
        	List<VoltBulkLoader> loaderList = m_vblGlobals.m_TableNameToLoader.get(m_tableName);
        	if (loaderList.size() == 1) {
        		m_vblGlobals.m_TableNameToLoader.remove(m_tableName);
        	}
        	else
        		loaderList.remove(this);

        	// First flush the tables
        	CountDownLatch tableWaitLatch = new CountDownLatch(m_lastPartitionTable-m_firstPartitionTable+1);
        	// keep one PerPartitionTable around so we can use it as the poisoned table for the PartitionProcessors
        	tmpTable = m_partitionTable[m_firstPartitionTable];
        	for (int i=m_firstPartitionTable; i<=m_lastPartitionTable; i++) {
	        	VoltBulkLoaderRow closeTableRow = new VoltBulkLoaderRow(this);
	        	closeTableRow.new CloseNotificationCallBack(m_partitionTable[i], tableWaitLatch);
	       		m_partitionTable[i].drainTableQueue(closeTableRow);
	       		// After this point we can no longer see the PerPartitionTable through m_partitionTable
	       	}
			try {
				// Wait for all our entries to be flushed from all the tables
				tableWaitLatch.await();
			} catch (InterruptedException e) {
			}
			assert(m_loaderBatchedRowCnt.get() == 0 && m_loaderQueuedRowCnt.get() == 0);
			
			CountDownLatch failureThreadLatch = new CountDownLatch(1);
			
			// At this point it is safe to assume that this BulkLoader has processed all non-failed Rows to completion
        	VoltBulkLoaderRow closeFailureProcRow = new VoltBulkLoaderRow(this);
        	closeFailureProcRow.new CloseNotificationCallBack(tmpTable, failureThreadLatch);
			m_failedQueue.add(closeFailureProcRow);
			try {
				failureThreadLatch.await();
			} catch (InterruptedException e) {
			}
        }

		assert(m_failedBatchQueuedRowCnt.get() == 0 && m_failedBatchSentRowCnt.get() == 0);

        // Remove all VoltBulkLoader-specific state in the client if there are no others 
        synchronized (m_clientImpl) {
        	if (m_clientImpl.isLastTerminatingVoltBulkLoader()) {
				m_vblGlobals.m_shutdownPartitionProcessors = true;
        		for (int i=0; i<m_maxPartitionProcessors; i++) {
        			m_vblGlobals.m_partitionProcessors[i].m_PendingTables.add(tmpTable);
        		}
            	try {
    				for (Thread th : m_vblGlobals.m_spawnedPartitionProcessors) {
    					th.join();
    				}
    			} catch (InterruptedException e) {
    			}
        	}
		}
	}
	
	/**
	 * As other instances of VoltBulkLoader working on the same table could alter the size
	 * of batches, this method provides the means to examine the current batch size.
	 * 
	 * @return The size of batches currently being submitted for this table
	 */
	public int getMaxBatchSize() {
		return m_partitionTable[m_firstPartitionTable].m_minBatchTriggerSize;
	}
	
	/**
	 * @return The number rows that have been received by this instance of VoltBulkLoader
	 *  but have not been processed by the Client
	 */
	public long getOutstandingRowCount() {
		return m_loaderQueuedRowCnt.get() + m_loaderBatchedRowCnt.get() +
				m_failedBatchQueuedRowCnt.get() + m_failedBatchSentRowCnt.get();
	}
	
	/**
	 * @return The number of rows that have been received by this instance of
	 *  VoltBulkLoader and processed by the Client including failed inserts that
	 *  have performed callbacks using BulkLoaderFailureCallBack.
	 */
	public long getCompletedRowCount() {
		return m_loaderCompletedCnt.get();
	}

}
