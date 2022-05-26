/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "common/ExecuteWithMpMemory.h"
#include "expressions/functionexpression.h"
#include "insertexecutor.h"
#include "plannodes/insertnode.h"
#include "storage/ConstraintFailureException.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"

namespace voltdb {
int64_t InsertExecutor::s_modifiedTuples;
std::string InsertExecutor::s_errorMessage{};
std::mutex InsertExecutor::s_errorMessageUpdateLocker{};

bool InsertExecutor::p_init(AbstractPlanNode* abstractNode, const ExecutorVector& executorVector) {
    VOLT_TRACE("init Insert Executor");

    m_node = dynamic_cast<InsertPlanNode*>(abstractNode);
    vassert(m_node);
    vassert(m_node->getTargetTable());
    vassert(m_node->getInputTableCount() == (m_node->isInline() ? 0 : 1));

    Table* targetTable = m_node->getTargetTable();
    m_isUpsert = m_node->isUpsert();

    //
    // The insert node's input schema is fixed.  But
    // if this is an inline node we don't set it here.
    // We let the parent node set it in p_execute_init.
    //
    // Also, we don't want to set the input table for inline
    // insert nodes.
    //
    if ( ! m_node->isInline()) {
        setDMLCountOutputTable(executorVector.limits());
        m_inputTable = dynamic_cast<AbstractTempTable*>(m_node->getInputTable()); //input table should be temptable
        vassert(m_inputTable);
    } else {
        m_inputTable = NULL;
    }

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(targetTable);
    m_partitionColumn = -1;
    StreamedTable *streamTarget = dynamic_cast<StreamedTable*>(targetTable);
    m_hasStreamView = false;
    if (streamTarget != NULL) {
        m_isStreamed = true;
        //See if we have any views.
        m_hasStreamView = streamTarget->hasViews();
        m_partitionColumn = streamTarget->partitionColumn();
    }
    if (m_isUpsert) {
        VOLT_TRACE("init Upsert Executor actually");
        vassert( ! m_node->isInline() );
        if (m_isStreamed) {
            VOLT_ERROR("UPSERT is not supported for Stream table %s", targetTable->name().c_str());
        }
        // look up the tuple whether it exists already
        if (persistentTarget->primaryKeyIndex() == NULL) {
            VOLT_ERROR("No primary keys were found in our target table '%s'",
                    targetTable->name().c_str());
        }
    }

    if (persistentTarget) {
        m_partitionColumn = persistentTarget->partitionColumn();
        m_replicatedTableOperation = persistentTarget->isReplicatedTable();
    }

    m_multiPartition = m_node->isMultiPartition();

    m_sourceIsPartitioned = m_node->sourceIsPartitioned();

    // allocate memory for template tuple, set defaults for all columns
    m_templateTupleStorage.init(targetTable->schema());


    TableTuple tuple = m_templateTupleStorage.tuple();

    std::set<int> fieldsExplicitlySet(m_node->getFieldMap().begin(), m_node->getFieldMap().end());
    // These default values are used for an INSERT including the INSERT sub-case of an UPSERT.
    // The defaults are purposely ignored in favor of existing column values
    // for the UPDATE subcase of an UPSERT.
    m_node->initTupleWithDefaultValues(m_engine, &m_memoryPool, fieldsExplicitlySet, tuple, m_nowFields);

    return true;
}

bool InsertExecutor::p_execute_init_internal(const TupleSchema *inputSchema,
        AbstractTempTable *newOutputTable, TableTuple &temp_tuple) {
    vassert(m_node == dynamic_cast<InsertPlanNode*>(m_abstractNode));
    vassert(m_node);
    vassert(inputSchema);
    vassert(m_node->isInline() || (m_inputTable == dynamic_cast<AbstractTempTable*>(m_node->getInputTable())));
    vassert(m_node->isInline() || m_inputTable);

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    // Update target table reference from table delegate
    m_targetTable = m_node->getTargetTable();
    vassert(m_targetTable);
    vassert(nullptr != dynamic_cast<PersistentTable*>(m_targetTable) ||
            nullptr != dynamic_cast<StreamedTable*>(m_targetTable));

    m_persistentTable = m_isStreamed ?
            NULL : static_cast<PersistentTable*>(m_targetTable);
    vassert((!m_persistentTable && !m_replicatedTableOperation) ||
            m_replicatedTableOperation == m_persistentTable->isReplicatedTable());

    m_upsertTuple = TableTuple(m_targetTable->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", m_node->isInline() ? "INLINE" : m_inputTable->debug().c_str());

    // Note that we need to clear static trackers: ENG-17091
    // https://issues.voltdb.com/browse/ENG-17091?focusedCommentId=50362&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-50362
    // count the number of successful inserts
    m_modifiedTuples = 0;

    m_tmpOutputTable = newOutputTable;
    vassert(m_tmpOutputTable);
    m_count_tuple = m_tmpOutputTable->tempTuple();

    // For export tables with no partition column,
    // if the data is from a replicated source,
    // only insert into one partition (0).
    // Other partitions can just return a 0 modified tuple count.
    // OTOH, if the data is coming from a (sub)query with
    // partitioned tables, perform the insert on every partition.
    if (m_partitionColumn == -1 && m_isStreamed && m_multiPartition &&
            !m_sourceIsPartitioned && m_engine->getPartitionId() != 0) {
        m_count_tuple.setNValue(0, ValueFactory::getBigIntValue(0L));
        // put the tuple into the output table
        m_tmpOutputTable->insertTuple(m_count_tuple);
        return false;
    }
    m_templateTuple = m_templateTupleStorage.tuple();

    for (auto iter: m_nowFields) {
        m_templateTuple.setNValue(iter, NValue::callConstant<FUNC_CURRENT_TIMESTAMP>());
    }

    VOLT_DEBUG("Initializing insert executor to insert into %s table %s",
            static_cast<PersistentTable*>(m_targetTable)->isReplicatedTable() ? "replicated" : "partitioned",
            m_targetTable->name().c_str());
    VOLT_DEBUG("This is a %s insert on partition with id %d",
            m_node->isInline() ? "inline"
            : (m_node->getChildren()[0]->getPlanNodeType() == PlanNodeType::Materialize ?
                "single-row" : "multi-row"),
            m_engine->getPartitionId());
    VOLT_DEBUG("Offset of partition column is %d", m_partitionColumn);
    //
    // Return a tuple whose schema we can use as an
    // input.
    //
    m_tempPool = ExecutorContext::getTempStringPool();
    char *storage = static_cast<char *>(m_tempPool->allocateZeroes(inputSchema->tupleLength() + TUPLE_HEADER_SIZE));
    temp_tuple = TableTuple(storage, inputSchema);
    return true;
}

bool InsertExecutor::p_execute_init(const TupleSchema *inputSchema,
        AbstractTempTable *newOutputTable, TableTuple &temp_tuple) {
    bool rslt = p_execute_init_internal(inputSchema, newOutputTable, temp_tuple);
    if (m_replicatedTableOperation &&
            SynchronizedThreadLock::countDownGlobalTxnStartCount(m_engine->isLowestSite())) {
        // Need to set this here for inlined inserts in case there are no inline inserts
        // and finish is called right after this
        s_modifiedTuples = 0;
        SynchronizedThreadLock::signalLowestSiteFinished();
    }
    return rslt;
}

void InsertExecutor::p_execute_tuple_internal(TableTuple &tuple) {
    const std::vector<int>& fieldMap = m_node->getFieldMap();
    std::size_t mapSize = fieldMap.size();

    for (int i = 0; i < mapSize; ++i) {
        // Most executors will just call setNValue instead of
        // setNValueAllocateForObjectCopies.
        //
        // However, We need to call
        // setNValueAllocateForObjectCopies here.  Sometimes the
        // input table's schema has an inlined string field, and
        // it's being assigned to the target table's non-inlined
        // string field.  In this case we need to tell the NValue
        // where to allocate the string data.
        // For an "upsert", this templateTuple setup has two effects --
        // It sets the primary key column(s) and it sets the
        // updated columns to their new values.
        // If the primary key value (combination) is new, the
        // templateTuple is the exact combination of new values
        // and default values required by the insert.
        // If the primary key value (combination) already exists,
        // only the NEW values stored on the templateTuple get updated
        // in the existing tuple and its other columns keep their existing
        // values -- the DEFAULT values that are stored in templateTuple
        // DO NOT get copied to an existing tuple.
        m_templateTuple.setNValueAllocateForObjectCopies(
                fieldMap[i], tuple.getNValue(i), m_tempPool);
    }

    VOLT_TRACE("Inserting tuple '%s' into target table '%s' with table schema: %s",
               m_templateTuple.debug(m_targetTable->name()).c_str(), m_targetTable->name().c_str(),
               m_targetTable->schema()->debug().c_str());

    // If there is a partition column for the target table
    if (m_partitionColumn != -1) {
        // get the value for the partition column
        NValue value = m_templateTuple.getNValue(m_partitionColumn);
        bool isLocal = m_engine->isLocalSite(value);

        // if it doesn't map to this partiton
        if (!isLocal) {
            if (m_multiPartition) {
                // The same row is presumed to also be generated
                // on some other partition, where the partition key
                // belongs.
                return;
            }
            // When a streamed table has no views, let an SP insert execute.
            // This is backward compatible with when there were only export
            // tables with no views on them.
            // When there are views, be strict and throw mispartitioned
            // tuples to force partitioned data to be generated only
            // where partitioned view rows are maintained.
            if (!m_isStreamed || m_hasStreamView) {
                throw ConstraintFailureException(m_targetTable, m_templateTuple,
                        "Mispartitioned tuple in single-partition insert statement.");
            }
        }
    }

    if (m_isUpsert) {
        // upsert execution logic
        vassert(m_persistentTable->primaryKeyIndex() != NULL);
        TableTuple existsTuple = m_persistentTable->lookupTupleByValues(m_templateTuple);

        if (!existsTuple.isNullTuple()) {
            // The tuple exists already, update (only) the templateTuple columns
            // that were initialized from the input tuple via the field map.
            // Technically, this includes setting primary key values,
            // but they are getting set to equivalent values, so that's OK.
            // A simple setNValue works here because any required object
            // allocations were handled when copying the input values into
            // the templateTuple.
            m_upsertTuple.move(m_templateTuple.address());
            TableTuple &tempTuple = m_persistentTable->copyIntoTempTuple(existsTuple);
            for (int i = 0; i < mapSize; ++i) {
                tempTuple.setNValue(fieldMap[i], m_templateTuple.getNValue(fieldMap[i]));
            }
            m_persistentTable->updateTupleWithSpecificIndexes(
                    existsTuple, tempTuple, m_persistentTable->allIndexes());
            // successfully updated
            ++m_modifiedTuples;
            return;
        }
        // else, the primary key did not match,
        // so fall through to the "insert" logic
    }

    m_targetTable->insertTuple(m_templateTuple);
    VOLT_TRACE("Target table:\n%s\n", m_targetTable->debug().c_str());
    // successfully inserted
    ++m_modifiedTuples;
}

void InsertExecutor::p_execute_tuple(TableTuple &tuple) {
    // This should only be called from inlined insert executors because we have to change contexts every time
    ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(
            m_replicatedTableOperation, m_engine->isLowestSite(),
            []() {
            s_modifiedTuples = -1l;
            s_errorMessage.clear();
            });
    if (possiblySynchronizedUseMpMemory.okToExecute()) {
        p_execute_tuple_internal(tuple);
        if (m_replicatedTableOperation) {
            s_modifiedTuples = m_modifiedTuples;
        }
    } else if (s_modifiedTuples == -1) {
        // An exception was thrown on the lowest site thread and we need to throw here as well so
        // all threads are in the same state
        throwSerializableTypedEEException(
                VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE,
                "Replicated table insert threw an unknown exception on other thread for table %s",
                m_targetTable->name().c_str());
    }
}

void InsertExecutor::p_execute_finish() {
    if (m_replicatedTableOperation) {
        // Use the static value assigned above to propagate the result to the other engines
        // that skipped the replicated table work
        vassert(s_modifiedTuples != -1);
        m_modifiedTuples = s_modifiedTuples;
    }
    m_count_tuple.setNValue(0, ValueFactory::getBigIntValue(m_modifiedTuples));
    // put the tuple into the output table
    m_tmpOutputTable->insertTuple(m_count_tuple);

    // add to the planfragments count of modified tuples
    m_engine->addToTuplesModified(m_modifiedTuples);
    VOLT_DEBUG("Finished inserting %" PRId64 " tuples", m_modifiedTuples);
    VOLT_TRACE("InsertExecutor output table:\n%s\n", m_tmpOutputTable->debug().c_str());
    VOLT_TRACE("InsertExecutor target table:\n%s\n", m_targetTable->debug().c_str());
}

bool InsertExecutor::p_execute(const NValueArray &params) {
   //
   // See p_execute_init above.  If we are inserting a
   // replicated table into an export table with no partition column,
   // we only insert on one site.  For all other sites we just
   // do nothing.
   //
   TableTuple inputTuple;
   const TupleSchema *inputSchema = m_inputTable->schema();
   if (p_execute_init_internal(inputSchema, m_tmpOutputTable, inputTuple)) {
      ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(
            m_replicatedTableOperation, m_engine->isLowestSite(),
            []() { s_modifiedTuples = -1l; });
      if (possiblySynchronizedUseMpMemory.okToExecute()) {
         //
         // An insert is quite simple really. We just loop through our m_inputTable
         // and insert any tuple that we find into our targetTable. It doesn't get any easier than that!
         //
         TableIterator iterator = m_inputTable->iterator();
         try {
            while (iterator.next(inputTuple)) {
               p_execute_tuple_internal(inputTuple);
            }
         } catch (ConstraintFailureException const& e) {
             if (m_replicatedTableOperation) {
                 s_errorMessage = e.what();
             }
            throw;
         }
         if (m_replicatedTableOperation) {
            s_modifiedTuples = m_modifiedTuples;
         }
      } else if (s_modifiedTuples == -1) {
         // An exception was thrown on the lowest site thread and we need to throw here as well so
         // all threads are in the same state
         char msg[4096];
         if (!s_errorMessage.empty()) {
            std::lock_guard<std::mutex> g(s_errorMessageUpdateLocker);
            strcpy(msg, s_errorMessage.c_str());
         } else {
            snprintf(msg, sizeof msg,
                    "Replicated table insert threw an unknown exception on other thread for table %s",
                    m_targetTable->name().c_str());
         }
         msg[sizeof msg - 1] = '\0';
         VOLT_DEBUG("%s", msg);
         // NOTE!!! Cannot throw any other types like ConstraintFailureException
         throw SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE, msg);
      }
   }

   p_execute_finish();
   return true;
}

InsertExecutor *getInlineInsertExecutor(const AbstractPlanNode *node) {
    InsertExecutor *answer = NULL;
    InsertPlanNode *insertNode = dynamic_cast<InsertPlanNode *>(node->getInlinePlanNode(PlanNodeType::Insert));
    if (insertNode) {
        answer = dynamic_cast<InsertExecutor *>(insertNode->getExecutor());
    }
    return answer;
}
}
