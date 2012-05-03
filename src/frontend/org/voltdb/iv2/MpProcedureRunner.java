/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.Expectation;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.ProcedureStatsCollector;
import org.voltdb.SQLStmt;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.compiler.ProcedureCompiler;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class MpProcedureRunner extends ProcedureRunner {

    private static final VoltLogger log = new VoltLogger("HOST");

    MpProcedureRunner(VoltProcedure procedure,
                    SiteProcedureConnection site,
                    Procedure catProc,
                    HsqlBackend hsql) {
        super(procedure, site, catProc, hsql);
    }

    @Override
    protected VoltTable[] executeQueriesInABatch(List<QueuedSQL> batch,
                                                 boolean isFinalSQL)
    {
        final int batchSize = batch.size();

        VoltTable[] results = null;

        if (batchSize == 0)
            return new VoltTable[] {};

        // IF THIS IS HSQL, RUN THE QUERIES DIRECTLY IN HSQL
        if (!m_isNative) {
            results = new VoltTable[batchSize];
            int i = 0;
            for (QueuedSQL qs : batch) {
                results[i++] = m_hsql.runSQLWithSubstitutions(qs.stmt, qs.params);
            }
        }
        else {
            results = slowPath(batch, isFinalSQL);
        }

        // check expectations
        int i = 0; for (QueuedSQL qs : batch) {
            Expectation.check(m_procedureName, qs.stmt.getText(),
                    i, qs.expectation, results[i]);
            i++;
        }

        // clear the queued sql list for the next call
        batch.clear();

        return results;
    }

/*
    DependencyPair executePlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params,
            ExecutionSite.SystemProcedureExecutionContext context) {
        setupTransaction(txnState);
        assert (m_procedure instanceof VoltSystemProcedure);
        VoltSystemProcedure sysproc = (VoltSystemProcedure) m_procedure;
        return sysproc.executePlanFragment(dependencies, fragmentId, params, context);
    }
*/

   private VoltTable[] executeQueriesInIndividualBatches(List<QueuedSQL> batch, boolean finalTask) {
       assert(batch.size() > 0);

       VoltTable[] retval = new VoltTable[batch.size()];

       ArrayList<QueuedSQL> microBatch = new ArrayList<QueuedSQL>();

       for (int i = 0; i < batch.size(); i++) {
           QueuedSQL queuedSQL = batch.get(i);
           assert(queuedSQL != null);

           microBatch.add(queuedSQL);

           boolean isThisLoopFinalTask = finalTask && (i == (batch.size() - 1));
           assert(microBatch.size() == 1);
           VoltTable[] results = executeQueriesInABatch(microBatch, isThisLoopFinalTask);
           assert(results != null);
           assert(results.length == 1);
           retval[i] = results[0];

           microBatch.clear();
       }

       return retval;
   }

   private VoltTable[] slowPath(List<QueuedSQL> batch, boolean finalTask) {
       /*
        * Determine if reads and writes are mixed. Can't mix reads and writes
        * because the order of execution is wrong when replicated tables are involved
        * due to ENG-1232
        */
       boolean hasRead = false;
       boolean hasWrite = false;
       for (int i = 0; i < batch.size(); ++i) {
           final SQLStmt stmt = batch.get(i).stmt;
           if (stmt.catStmt.getReadonly()) {
               hasRead = true;
           } else {
               hasWrite = true;
           }
       }
       /*
        * If they are all reads or all writes then we can use the batching slow path
        * Otherwise the order of execution will be interleaved incorrectly so we have to do
        * each statement individually.
        */
       if (hasRead && hasWrite) {
           return executeQueriesInIndividualBatches(batch, finalTask);
       }

       // assume all reads or all writes from this point

       VoltTable[] results = new VoltTable[batch.size()];

       // the set of dependency ids for the expected results of the batch
       // one per sql statment
       int[] depsToResume = new int[batch.size()];

       // these dependencies need to be received before the local stuff can run
       int[] depsForLocalTask = new int[batch.size()];

       // the list of frag ids to run locally
       long[] localFragIds = new long[batch.size()];

       // the list of frag ids to run remotely
       ArrayList<Long> distributedFragIds = new ArrayList<Long>();
       ArrayList<Integer> distributedOutputDepIds = new ArrayList<Integer>();

       // the set of parameters for the local tasks
       ByteBuffer[] localParams = new ByteBuffer[batch.size()];

       // the set of parameters for the distributed tasks
       ArrayList<ByteBuffer> distributedParams = new ArrayList<ByteBuffer>();

       // check if all local fragment work is non-transactional
       boolean localFragsAreNonTransactional = false;

       // iterate over all sql in the batch, filling out the above data structures
       for (int i = 0; i < batch.size(); ++i) {
           QueuedSQL queuedSQL = batch.get(i);

           // Figure out what is needed to resume the proc
           int collectorOutputDepId = m_txnState.getNextDependencyId();
           depsToResume[i] = collectorOutputDepId;

           // Build the set of params for the frags
           FastSerializer fs = new FastSerializer();
           try {
               fs.writeObject(queuedSQL.params);
           } catch (IOException e) {
               throw new RuntimeException("Error serializing parameters for SQL statement: " +
                                          queuedSQL.stmt.getText() + " with params: " +
                                          queuedSQL.params.toJSONString(), e);
           }
           ByteBuffer params = fs.getBuffer();
           assert(params != null);

           // populate the actual lists of fragments and params
           int numFrags = queuedSQL.stmt.catStmt.getFragments().size();
           assert(numFrags > 0);
           assert(numFrags <= 2);

           /*
            * This numfrags == 1 code is for routing multi-partition reads of a
            * replicated table to the local site. This was a broken performance optimization.
            * see https://issues.voltdb.com/browse/ENG-1232
            * The problem is that the fragments for the replicated read are not correctly interleaved with the
            * distributed writes to the replicated table that might be in the same batch of SQL statements.
            * We do end up doing the replicated read locally but we break up the batches in the face of mixed
            * reads and writes
            */
           if (numFrags == 1) {
               for (PlanFragment frag : queuedSQL.stmt.catStmt.getFragments()) {
                   assert(frag != null);
                   assert(frag.getHasdependencies() == false);

                   localFragIds[i] = CatalogUtil.getUniqueIdForFragment(frag);
                   localParams[i] = params;

                   // if any frag is transactional, update this check
                   if (frag.getNontransactional() == true)
                       localFragsAreNonTransactional = true;
               }
               depsForLocalTask[i] = -1;
           }
           else {
               for (PlanFragment frag : queuedSQL.stmt.catStmt.getFragments()) {
                   assert(frag != null);

                   // frags with no deps are usually collector frags that go to all partitions
                   if (frag.getHasdependencies() == false) {
                       distributedFragIds.add(CatalogUtil.getUniqueIdForFragment(frag));
                       distributedParams.add(params);
                   }
                   // frags with deps are usually aggregator frags
                   else {
                       localFragIds[i] = CatalogUtil.getUniqueIdForFragment(frag);
                       localParams[i] = params;
                       assert(frag.getHasdependencies());
                       int outputDepId =
                               m_txnState.getNextDependencyId() | DtxnConstants.MULTIPARTITION_DEPENDENCY;
                       depsForLocalTask[i] = outputDepId;
                       distributedOutputDepIds.add(outputDepId);

                       // if any frag is transactional, update this check
                       if (frag.getNontransactional() == true)
                           localFragsAreNonTransactional = true;
                   }
               }
           }
       }

       // convert a bunch of arraylists into arrays
       // this should be easier, but we also want little-i ints rather than Integers
       long[] distributedFragIdArray = new long[distributedFragIds.size()];
       int[] distributedOutputDepIdArray = new int[distributedFragIds.size()];
       ByteBuffer[] distributedParamsArray = new ByteBuffer[distributedFragIds.size()];

       assert(distributedFragIds.size() == distributedParams.size());

       for (int i = 0; i < distributedFragIds.size(); i++) {
           distributedFragIdArray[i] = distributedFragIds.get(i);
           distributedOutputDepIdArray[i] = distributedOutputDepIds.get(i);
           distributedParamsArray[i] = distributedParams.get(i);
       }

       // instruct the dtxn what's needed to resume the proc
       m_txnState.setupProcedureResume(finalTask, depsToResume);

       // create all the local work for the transaction
       FragmentTaskMessage localTask = new FragmentTaskMessage(m_txnState.initiatorHSId,
                                                 m_site.getCorrespondingSiteId(),
                                                 m_txnState.txnId,
                                                 m_txnState.isReadOnly(),
                                                 localFragIds,
                                                 depsToResume,
                                                 localParams,
                                                 false);
       for (int i = 0; i < depsForLocalTask.length; i++) {
           if (depsForLocalTask[i] < 0) continue;
           localTask.addInputDepId(i, depsForLocalTask[i]);
       }

       // note: non-transactional work only helps us if it's final work
       m_txnState.createLocalFragmentWork(localTask, localFragsAreNonTransactional && finalTask);

       // create and distribute work for all sites in the transaction
       FragmentTaskMessage distributedTask = new FragmentTaskMessage(m_txnState.initiatorHSId,
                                                       m_site.getCorrespondingSiteId(),
                                                       m_txnState.txnId,
                                                       m_txnState.isReadOnly(),
                                                       distributedFragIdArray,
                                                       distributedOutputDepIdArray,
                                                       distributedParamsArray,
                                                       finalTask);

       m_txnState.createAllParticipatingFragmentWork(distributedTask);

       // recursively call recurableRun and don't allow it to shutdown
       Map<Integer,List<VoltTable>> mapResults =
           m_site.recursableRun(m_txnState);

       assert(mapResults != null);
       assert(depsToResume != null);
       assert(depsToResume.length == batch.size());

       // build an array of answers, assuming one result per expected id
       for (int i = 0; i < batch.size(); i++) {
           List<VoltTable> matchingTablesForId = mapResults.get(depsToResume[i]);
           assert(matchingTablesForId != null);
           assert(matchingTablesForId.size() == 1);
           results[i] = matchingTablesForId.get(0);

           if (batch.get(i).stmt.catStmt.getReplicatedtabledml()) {
               long newVal = results[i].asScalarLong() / m_site.getReplicatedDMLDivisor();
               results[i] = new VoltTable(new VoltTable.ColumnInfo("modified_tuples", VoltType.BIGINT));
               results[i].addRow(newVal);
           }
       }

       return results;
   }
}
