/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include "BinaryLogSink.h"

#include "ConstraintFailureException.h"
#include "DRTableNotFoundException.h"
#include "persistenttable.h"
#include "streamedtable.h"
#include "tablefactory.h"
#include "temptable.h"

#include "catalog/database.h"

#include "common/debuglog.h"
#include "common/ExecuteWithMpMemory.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "common/UniqueId.hpp"
#include "indexes/tableindex.h"

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include <crc/crc32c.h>

#include <string>

#define throwDRTableNotFoundException(tableHash, ...)         \
    do {                                                      \
        char _msg_[1024];                                     \
        snprintf(_msg_, 1024, __VA_ARGS__);                   \
        _msg_[sizeof _msg_ - 1] = '\0';                       \
        throw DRTableNotFoundException(tableHash, _msg_);     \
    } while (false)

namespace voltdb {

const static std::string EXISTING_TABLE = "existing_table";
const static std::string EXPECTED_TABLE = "expected_table";
const static std::string NEW_TABLE = "new_table";
const static std::string DELETED_TABLE = "deleted_table";

// column indices of DR conflict export table
const static int DR_ROW_TYPE_COLUMN_INDEX = 0;
const static int DR_LOG_ACTION_COLUMN_INDEX = 1;
const static int DR_CONFLICT_COLUMN_INDEX = 2;
const static int DR_CONFLICTS_ON_PK_COLUMN_INDEX = 3;
const static int DR_ACTION_DECISION_COLUMN_INDEX = 4;
const static int DR_REMOTE_CLUSTER_ID_COLUMN_INDEX = 5;
const static int DR_REMOTE_TIMESTAMP_COLUMN_INDEX = 6;
const static int DR_DIVERGENCE_COLUMN_INDEX = 7;
const static int DR_TABLE_NAME_COLUMN_INDEX = 8;
const static int DR_CURRENT_CLUSTER_ID_COLUMN_INDEX = 9;
const static int DR_CURRENT_TIMESTAMP_COLUMN_INDEX = 10;
const static int DR_TUPLE_COLUMN_INDEX = 11;

const static int DECISION_BIT = 1;
const static int RESOLVED_BIT = 1 << 1;

// a c++ style way to limit access from outside this file
namespace {

TIMER_LVLS(applyLogs, micro, 100000, 10000, 1000, 100)

// Utility functions to convert types to strings. Each type string has a fixed
// length. Check the schema of the conflict export table for the limits.
// 3 letters
inline std::string DRConflictRowTypeStr(DRConflictRowType type) {
    switch (type) {
    case EXISTING_ROW:
        return "EXT";
    case EXPECTED_ROW:
        return "EXP";
    case NEW_ROW:
        return "NEW";
    case DELETED_ROW:
        return "DEL";
    default:
        return "";
    }
}

// 1 letter
inline std::string DRRecordTypeStr(DRRecordType type) {
    switch (type) {
    case DR_RECORD_INSERT:
        return "I";
    case DR_RECORD_DELETE:
    case DR_RECORD_DELETE_BY_INDEX:
        return "D";
    case DR_RECORD_UPDATE:
    case DR_RECORD_UPDATE_BY_INDEX:
        return "U";
    case DR_RECORD_TRUNCATE_TABLE:
        return "T";
    default:
        return "";
    }
}

// 4 letters
inline std::string DRConflictTypeStr(DRConflictType type) {
    switch (type) {
    case NO_CONFLICT:
        return "NONE";
    case CONFLICT_CONSTRAINT_VIOLATION:
        return "CNST";
    case CONFLICT_EXPECTED_ROW_MISSING:
        return "MISS";
    case CONFLICT_EXPECTED_ROW_MISMATCH:
        return "MSMT";
    default:
        return "";
    }
}

// 1 letter
inline std::string DRDecisionStr(DRRowDecision type) {
    switch (type) {
    case ACCEPT:
        return "A";
    case REJECT:
        return "R";
    default:
        return "";
    }
}

// 1 letter
inline std::string DRDivergenceStr(DRDivergence type) {
    switch (type) {
    case NOT_DIVERGE:
        return "C";
    case DIVERGE:
        return "D";
    default:
        return "";
    }
}

bool isApplyNewRow(int32_t retval) {
    return (retval & DECISION_BIT) == DECISION_BIT;
}

bool isResolved(int32_t retval) {
    return (retval & RESOLVED_BIT) == RESOLVED_BIT;
}

void setConflictOutcome(boost::shared_ptr<TempTable> metadataTable, bool acceptRemoteChange, bool convergent) {
    TableTuple tuple(metadataTable->schema());
    TableIterator iter = metadataTable->iterator();
    while (iter.next(tuple)) {
        tuple.setNValue(DR_ACTION_DECISION_COLUMN_INDEX,
                        ValueFactory::getTempStringValue(DRDecisionStr(acceptRemoteChange ? ACCEPT : REJECT)));
        tuple.setNValue(DR_DIVERGENCE_COLUMN_INDEX,
                        ValueFactory::getTempStringValue(DRDivergenceStr(convergent ? NOT_DIVERGE : DIVERGE)));
    }
}

void exportTuples(StreamedTable *exportTable, Table *metaTable, Table *tupleTable) {
    TableTuple tempMetaTuple(exportTable->schema());
    TableIterator metaIter = metaTable->iterator();
    if (!tupleTable) {
        while (metaIter.next(tempMetaTuple)) {
            exportTable->insertTuple(tempMetaTuple);
        }
    }
    else {
        TableTuple tempTupleTuple(tupleTable->schema());
        TableIterator tupleIter = tupleTable->iterator();
        while (metaIter.next(tempMetaTuple) && tupleIter.next(tempTupleTuple)) {
            tempMetaTuple.setNValue(DR_TUPLE_COLUMN_INDEX,
                                    ValueFactory::getTempStringValue(tempTupleTuple.toJsonString(tupleTable->getColumnNames())));
            exportTable->insertTuple(tempMetaTuple);
        }
    }
}

typedef std::pair<boost::shared_ptr<TableTuple>, bool>  LabeledTableTuple;

/**
   * Find all rows in a @table that conflict with the @searchTuple (unique key violation) except the @expectedTuple
   * All conflicting rows are put into @conflictRows.
   */
void findConflictTuple(PersistentTable *table, const TableTuple *existingTuple, const TableTuple *searchTuple,
                       const TableTuple *expectedTuple, std::vector<LabeledTableTuple> &conflictRows) {
    std::unordered_set<char*> redundancyFilter;
    for(TableIndex* index : table->allIndexes()) {
        if (index->isUniqueIndex()) {
            IndexCursor cursor(index->getTupleSchema());
            if (index->moveToKeyByTuple(searchTuple, cursor)) {
                TableTuple conflictTuple = index->nextValueAtKey(cursor);
                if (expectedTuple) {
                    if (expectedTuple->equals(conflictTuple)) {
                        // exclude the expected tuple in update
                        continue;
                    }
                    if (existingTuple && existingTuple->equals(conflictTuple)) {
                        // in update this row was already listed in existingTableForDelete,
                        // don't include it in existingTableForInsert.
                        continue;
                    }
                }
                if (redundancyFilter.find(conflictTuple.address()) != redundancyFilter.end()) {
                    // skip the conflict tuples that are already found
                    continue;
                }
                conflictRows.push_back(std::make_pair(boost::shared_ptr<TableTuple>(new TableTuple(conflictTuple)),
                                                      table->primaryKeyIndex() == index ? true : false));
                redundancyFilter.insert(conflictTuple.address());
            }
        }
    }
}

/**
 * create conflict export tuple from the conflict tuple
 */
void createConflictExportTuple(TempTable *outputMetaTable, TempTable *outputTupleTable, PersistentTable *drTable,
        Pool *pool, const TableTuple *tupleToBeWrote, DRConflictOnPK conflictOnPKType, DRRecordType actionType,
        DRConflictType conflictType, DRConflictRowType rowType, int64_t remoteUniqueId, int32_t remoteClusterId) {
    vassert(ExecutorContext::getExecutorContext() != NULL);

    int32_t localClusterId = ExecutorContext::getExecutorContext()->drClusterId();
    int64_t localTsCounter = UniqueId::timestampSinceUnixEpoch(ExecutorContext::getExecutorContext()->currentUniqueId());

    TableTuple tempMetaTuple = outputMetaTable->tempTuple();

    tempMetaTuple.setNValue(DR_ROW_TYPE_COLUMN_INDEX, ValueFactory::getTempStringValue(DRConflictRowTypeStr(rowType)));
    tempMetaTuple.setNValue(DR_LOG_ACTION_COLUMN_INDEX, ValueFactory::getTempStringValue(DRRecordTypeStr(actionType)));
    tempMetaTuple.setNValue(DR_CONFLICT_COLUMN_INDEX, ValueFactory::getTempStringValue(DRConflictTypeStr(conflictType)));
    tempMetaTuple.setNValue(DR_CONFLICTS_ON_PK_COLUMN_INDEX, ValueFactory::getTinyIntValue(conflictOnPKType));
    tempMetaTuple.setNValue(DR_ACTION_DECISION_COLUMN_INDEX, ValueFactory::getTempStringValue(DRDecisionStr(REJECT)));

    // For deleted tuple we only know the cluster id and the timestamp when the deletion occurs
    if (rowType == DELETED_ROW) {
        tempMetaTuple.setNValue(DR_REMOTE_CLUSTER_ID_COLUMN_INDEX, ValueFactory::getTinyIntValue(remoteClusterId));
        tempMetaTuple.setNValue(DR_REMOTE_TIMESTAMP_COLUMN_INDEX, ValueFactory::getBigIntValue(UniqueId::timestampSinceUnixEpoch(remoteUniqueId)));
    }
    else {
        NValue hiddenValue = tupleToBeWrote->getHiddenNValue(drTable->getDRTimestampColumnIndex());
        tempMetaTuple.setNValue(DR_REMOTE_CLUSTER_ID_COLUMN_INDEX, ValueFactory::getTinyIntValue((ExecutorContext::getClusterIdFromHiddenNValue(hiddenValue))));
        tempMetaTuple.setNValue(DR_REMOTE_TIMESTAMP_COLUMN_INDEX, ValueFactory::getBigIntValue(ExecutorContext::getDRTimestampFromHiddenNValue(hiddenValue)));
        // Must have to deep copy non-inlined data, because tempTuple may be overwritten by following call of this function.
        outputTupleTable->insertTempTupleDeepCopy(*tupleToBeWrote, pool);
    }
    tempMetaTuple.setNValue(DR_DIVERGENCE_COLUMN_INDEX, ValueFactory::getTempStringValue(DRDivergenceStr(NOT_DIVERGE)));
    tempMetaTuple.setNValue(DR_TABLE_NAME_COLUMN_INDEX, ValueFactory::getTempStringValue(drTable->name()));
    tempMetaTuple.setNValue(DR_CURRENT_CLUSTER_ID_COLUMN_INDEX, ValueFactory::getTinyIntValue(localClusterId));
    tempMetaTuple.setNValue(DR_CURRENT_TIMESTAMP_COLUMN_INDEX, ValueFactory::getBigIntValue(localTsCounter));
    tempMetaTuple.setNValue(DR_TUPLE_COLUMN_INDEX, ValueFactory::getNullStringValue());
    // Must have to deep copy non-inlined data, because tempTuple may be overwritten by following call of this function.
    outputMetaTable->insertTempTupleDeepCopy(tempMetaTuple, pool);

}

// iterate all tables and push them into export table
void exportDRConflict(StreamedTable *exportTable,
                      TempTable *existingMetaTableForDelete, TempTable *existingTupleTableForDelete,
                      TempTable *expectedMetaTableForDelete, TempTable *expectedTupleTableForDelete,
                      TempTable *deletedMetaTableForDelete,
                      TempTable *existingMetaTableForInsert, TempTable *existingTupleTableForInsert,
                      TempTable *newMetaTableForInsert, TempTable *newTupleTableForInsert) {
    vassert(exportTable != NULL);
    vassert((existingMetaTableForDelete == NULL && existingTupleTableForDelete == NULL) ||
           (existingMetaTableForDelete != NULL && existingTupleTableForDelete != NULL));
    vassert((expectedMetaTableForDelete == NULL && expectedTupleTableForDelete == NULL) ||
           (expectedMetaTableForDelete != NULL && expectedTupleTableForDelete != NULL));
    vassert((existingMetaTableForInsert == NULL && existingTupleTableForInsert == NULL) ||
           (existingMetaTableForInsert != NULL && existingTupleTableForInsert != NULL));
    vassert((newMetaTableForInsert == NULL && newTupleTableForInsert == NULL) ||
           (newMetaTableForInsert != NULL && newTupleTableForInsert != NULL));

    if (existingMetaTableForDelete) {
        exportTuples(exportTable, existingMetaTableForDelete, existingTupleTableForDelete);
    }

    if (expectedMetaTableForDelete) {
        exportTuples(exportTable, expectedMetaTableForDelete, expectedTupleTableForDelete);
    }

    if (deletedMetaTableForDelete) {
        exportTuples(exportTable, deletedMetaTableForDelete, NULL);
    }

    if (existingMetaTableForInsert) {
        exportTuples(exportTable, existingMetaTableForInsert, existingTupleTableForInsert);
    }

    if (newMetaTableForInsert) {
        exportTuples(exportTable, newMetaTableForInsert, newTupleTableForInsert);
    }
}

bool handleConflict(VoltDBEngine *engine, PersistentTable *drTable, Pool *pool, TableTuple *existingTuple,
        const TableTuple *expectedTuple, TableTuple *newTuple, int64_t uniqueId, int32_t remoteClusterId,
        DRRecordType actionType, DRConflictType deleteConflict, DRConflictType insertConflict, bool m_drIgnoreConflicts) {
    if (m_drIgnoreConflicts) {
        return true;
    }
    if (!engine) {
        return false;
    }
    StreamedTable* conflictExportTable;
    if (drTable->isReplicatedTable()) {
        conflictExportTable = engine->getReplicatedDRConflictStreamedTable();
    }
    else {
        conflictExportTable = engine->getPartitionedDRConflictStreamedTable();
    }
    if (!conflictExportTable) {
        return false;
    }

    // construct delete conflict
    boost::shared_ptr<TempTable> existingMetaTableForDelete;
    boost::shared_ptr<TempTable> existingTupleTableForDelete;
    boost::shared_ptr<TempTable> expectedMetaTableForDelete;
    boost::shared_ptr<TempTable> expectedTupleTableForDelete;
    boost::shared_ptr<TempTable> deletedMetaTableForDelete;
    if (deleteConflict != NO_CONFLICT) {
        existingMetaTableForDelete.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, conflictExportTable));
        existingTupleTableForDelete.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, drTable));
        if (existingTuple) {
            createConflictExportTuple(existingMetaTableForDelete.get(), existingTupleTableForDelete.get(),
                    drTable, pool, existingTuple, NOT_CONFLICT_ON_PK, actionType,
                    deleteConflict, EXISTING_ROW, uniqueId, remoteClusterId);
        }
    }
    if (expectedTuple) {
        expectedMetaTableForDelete.reset(TableFactory::buildCopiedTempTable(EXPECTED_TABLE, conflictExportTable));
        expectedTupleTableForDelete.reset(TableFactory::buildCopiedTempTable(EXPECTED_TABLE, drTable));
        createConflictExportTuple(expectedMetaTableForDelete.get(), expectedTupleTableForDelete.get(),
                drTable, pool, expectedTuple, NOT_CONFLICT_ON_PK, actionType,
                deleteConflict, EXPECTED_ROW, uniqueId, remoteClusterId);

        // Since in delete record we only has the before image of the deleted row, needs more information to tell
        // when was the deletion happen.
        if (actionType == DR_RECORD_DELETE) {
            deletedMetaTableForDelete.reset(TableFactory::buildCopiedTempTable(DELETED_TABLE, conflictExportTable));
            createConflictExportTuple(deletedMetaTableForDelete.get(), NULL,
                    drTable, pool, NULL, NOT_CONFLICT_ON_PK, actionType,
                    deleteConflict, DELETED_ROW, uniqueId, remoteClusterId);
        }
    }

    // find any rows conflict with the new row
    std::vector< LabeledTableTuple > existingRows;
    if (newTuple) {
        findConflictTuple(drTable, existingTuple, newTuple, actionType == DR_RECORD_UPDATE ? expectedTuple : NULL, existingRows);
        if (actionType == DR_RECORD_UPDATE) {
            if (existingRows.size() > 0) {
                insertConflict = CONFLICT_CONSTRAINT_VIOLATION; // update timestamp mismatch may trigger constraint violation conflict
            }
        }
    }
    // construct insert conflict
    boost::shared_ptr<TempTable> existingMetaTableForInsert;
    boost::shared_ptr<TempTable> existingTupleTableForInsert;
    boost::shared_ptr<TempTable> newMetaTableForInsert;
    boost::shared_ptr<TempTable> newTupleTableForInsert;
    if (insertConflict != NO_CONFLICT) {
        existingMetaTableForInsert.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, conflictExportTable));
        existingTupleTableForInsert.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, drTable));
        if (existingRows.size() > 0) {
            BOOST_FOREACH(LabeledTableTuple labeledTuple, existingRows) {
                createConflictExportTuple(existingMetaTableForInsert.get(), existingTupleTableForInsert.get(), drTable,
                        pool, labeledTuple.first.get(), labeledTuple.second ? CONFLICT_ON_PK : NOT_CONFLICT_ON_PK,
                        actionType, insertConflict, EXISTING_ROW, uniqueId, remoteClusterId);
            }
        }
    }

    if (newTuple) {
        vassert(ExecutorContext::getDRTimestampFromHiddenNValue(newTuple->getHiddenNValue(drTable->getDRTimestampColumnIndex()))
               == UniqueId::timestampSinceUnixEpoch(uniqueId));

        newMetaTableForInsert.reset(TableFactory::buildCopiedTempTable(NEW_TABLE, conflictExportTable));
        newTupleTableForInsert.reset(TableFactory::buildCopiedTempTable(NEW_TABLE, drTable));
        createConflictExportTuple(newMetaTableForInsert.get(), newTupleTableForInsert.get(),
                                  drTable, pool, newTuple, NOT_CONFLICT_ON_PK, actionType,
                                  insertConflict, NEW_ROW, uniqueId, remoteClusterId);
    }

    int retval = ExecutorContext::getPhysicalTopend()->reportDRConflict(engine->getPartitionId(),
                                                                        remoteClusterId,
                                                                        UniqueId::timestampSinceUnixEpoch(uniqueId),
                                                                        drTable->name(),
                                                                        drTable->isReplicatedTable(),
                                                                        actionType,
                                                                        deleteConflict,
                                                                        existingMetaTableForDelete.get(),
                                                                        existingTupleTableForDelete.get(),
                                                                        expectedMetaTableForDelete.get(),
                                                                        expectedTupleTableForDelete.get(),
                                                                        insertConflict,
                                                                        existingMetaTableForInsert.get(),
                                                                        existingTupleTableForInsert.get(),
                                                                        newMetaTableForInsert.get(),
                                                                        newTupleTableForInsert.get());
    bool applyRemoteChange = isApplyNewRow(retval);
    bool resolved = isResolved(retval);
    // if conflict is not resolved, don't delete any existing rows.
    vassert(resolved || !applyRemoteChange);

    if (existingMetaTableForDelete) {
        setConflictOutcome(existingMetaTableForDelete, applyRemoteChange, resolved);
    }
    if (expectedMetaTableForDelete) {
        setConflictOutcome(expectedMetaTableForDelete, applyRemoteChange, resolved);
    }
    if (deletedMetaTableForDelete) {
        setConflictOutcome(deletedMetaTableForDelete, applyRemoteChange, resolved);
    }
    if (existingMetaTableForInsert) {
        setConflictOutcome(existingMetaTableForInsert, applyRemoteChange, resolved);
    }
    if (newMetaTableForInsert) {
        setConflictOutcome(newMetaTableForInsert, applyRemoteChange, resolved);
    }

    if (applyRemoteChange) {
        if (deleteConflict != NO_CONFLICT) {
            if (existingTuple) {
                drTable->deleteTuple(*existingTuple, true);
            }
        }
        if (insertConflict != NO_CONFLICT) {
            BOOST_FOREACH(LabeledTableTuple tupleToDelete, existingRows) {
                drTable->deleteTuple(*tupleToDelete.first.get(), true);
            }
        }
        if (newTuple) {
            drTable->insertPersistentTuple(*newTuple, true);
        }
    }

    // For replicated table, pick partition 0 to export the conflicts.
    if (!drTable->isReplicatedTable() || engine->getPartitionId() == 0) {
        exportDRConflict(conflictExportTable, existingMetaTableForDelete.get(), existingTupleTableForDelete.get(),
                expectedMetaTableForDelete.get(), expectedTupleTableForDelete.get(),
                deletedMetaTableForDelete.get(),
                existingMetaTableForInsert.get(), existingTupleTableForInsert.get(),
                newMetaTableForInsert.get(), newTupleTableForInsert.get());
    }

    if (existingMetaTableForDelete.get()) {
        existingMetaTableForDelete.get()->deleteAllTempTupleDeepCopies();
    }
    if (existingTupleTableForDelete.get()) {
        existingTupleTableForDelete.get()->deleteAllTempTupleDeepCopies();
    }
    if (expectedMetaTableForDelete.get()) {
        expectedMetaTableForDelete.get()->deleteAllTempTupleDeepCopies();
    }
    if (expectedTupleTableForDelete.get()) {
        expectedTupleTableForDelete.get()->deleteAllTempTupleDeepCopies();
    }
    if (deletedMetaTableForDelete.get()) {
        deletedMetaTableForDelete.get()->deleteAllTempTupleDeepCopies();
    }
    if (existingMetaTableForInsert.get()) {
        existingMetaTableForInsert.get()->deleteAllTempTupleDeepCopies();
    }
    if (existingTupleTableForInsert.get()) {
        existingTupleTableForInsert.get()->deleteAllTempTupleDeepCopies();
    }
    if (newMetaTableForInsert.get()) {
        newMetaTableForInsert.get()->deleteAllTempTupleDeepCopies();
    }
    if (newTupleTableForInsert.get()) {
        newTupleTableForInsert.get()->deleteAllTempTupleDeepCopies();
    }

    return true;
}

inline void truncateTable(std::unordered_map<int64_t, PersistentTable*> &tables,
        VoltDBEngine *engine, int64_t tableHandle, std::string *tableName) {
    std::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
    if (tableIter == tables.end()) {
        throwDRTableNotFoundException(tableHandle, "Unable to find table %s hash %jd while applying binary log for truncate record",
                                      tableName->c_str(), (intmax_t) tableHandle);
    }

    PersistentTable *table = tableIter->second;

    table->truncateTable(engine);
}

} //end of anonymous namespace

/**
 * Utility class for managing a binary log
 */
class BinaryLog {
    friend class BinaryLogSink;

public:
    /**
     * @return a new BinaryLog * if len of log > 0 else returns null
     */
    static BinaryLog *create(const char *log) {
        int32_t logLen = readRawInt(log);
        if (logLen > 0) {
            return new BinaryLog(log, logLen);
        }
        return NULL;
    }

    /**
     * Construct a new BinaryLog.
     *
     * @param log array containing the binary log. Must not have a log of length 0.
     */
    BinaryLog(const char *log) :
            m_taskInfo(log + sizeof(int32_t), readRawInt(log)) {
        initialize(readRawInt(log));
    }

    /**
     * Skip any remaining logs in a transaction and call BinaryLog::validateEndTxn
     */
    void skipRecordsAndValidateTxn(VoltDBEngine *engine, int *crcErrorCount, int drCrcErrorIgnoreMax, bool drCrcErrorFatal) {
        m_taskInfo.getRawPointer(
                (m_txnStart + m_txnLen) - (m_taskInfo.getRawPointer() + DRTupleStream::END_RECORD_SIZE));
        DRRecordType __attribute__ ((unused)) type = readRecordType();
        vassert(type = DR_RECORD_END_TXN);
        validateEndTxn(engine, crcErrorCount, drCrcErrorIgnoreMax, drCrcErrorFatal);
    }

    /**
     * Validate that the sequence number and hash in the end transaction record match the rest of the transaction
     *
     * Note: DR_RECORD_END_TXN must already have been consumed prior to invoking this method
     */
    void validateEndTxn(VoltDBEngine *engine, int *crcErrorCount, int drCrcErrorIgnoreMax, bool drCrcErrorFatal) {
        int64_t tempSequenceNumber = m_taskInfo.readLong();
        if (tempSequenceNumber != m_sequenceNumber) {
            throwFatalException("Closing the wrong transaction inside a binary log segment. Expected %jd but found %jd",
                    (intmax_t )m_sequenceNumber, (intmax_t )tempSequenceNumber);
        }
        uint32_t checksum = m_taskInfo.readInt();
        vassert(m_taskInfo.getRawPointer() == m_txnStart + m_txnLen);
        validateChecksum(engine, checksum, m_txnStart, m_taskInfo.getRawPointer(), m_txnLen, isReplicatedTableLog(),
                         crcErrorCount, drCrcErrorIgnoreMax, drCrcErrorFatal);
    }

    void validateChecksum(VoltDBEngine *engine, uint32_t checksum,
                          const char *start, const char *end, int32_t txnLength, bool isMultiHash,
                          int *crcErrorCount, int drCrcErrorIgnoreMax, bool drCrcErrorFatal) {
        uint32_t recalculatedCRC = vdbcrc::crc32cInit();
        recalculatedCRC = vdbcrc::crc32c(recalculatedCRC, start, (txnLength - sizeof(recalculatedCRC)));
        recalculatedCRC = vdbcrc::crc32cFinish(recalculatedCRC);

        if (recalculatedCRC != checksum) {
            *crcErrorCount = *crcErrorCount + 1;
            char errMsg[1024];
            snprintf(errMsg, 1024, "CRC mismatch of DR log data %d and %d details "
                                   "(ignoreMax=%d) (txnLengthforcrc = %ld, buflenforcrc=%ld, multiHash = %s)",
                     checksum, recalculatedCRC, drCrcErrorIgnoreMax, (txnLength - sizeof(recalculatedCRC)),
                     (end - 4) - start, (isMultiHash ? "true" : "false"));
            ExecutorContext::getPhysicalTopend()->reportDRBuffer(engine->getPartitionId(), errMsg, start, txnLength);
            if (*crcErrorCount > drCrcErrorIgnoreMax) { // Break replication or fatal
                if (drCrcErrorFatal) {
                    throwFatalException("%s", errMsg);
                } else {
                    throwSerializableEEException("%s", errMsg);
                }
            }
            // else we commit.
        }
    }

    /**
     * Read the next transaction record from the log
     *
     * @return true if a new transaction record exists otherwise false
     */
    bool readNextTransaction() {
        if (!m_taskInfo.hasRemaining()) {
            return false;
        }

        m_txnStart = m_taskInfo.getRawPointer();

        const uint8_t drVersion = m_taskInfo.readByte();
        if (drVersion < DRTupleStream::COMPATIBLE_PROTOCOL_VERSION) {
            throwFatalException("Unsupported DR version %d", drVersion);
        }

        DRRecordType __attribute__ ((unused)) type = readRecordType();
        vassert(type == DR_RECORD_BEGIN_TXN);

        m_uniqueId = m_taskInfo.readLong();
        m_sequenceNumber = m_taskInfo.readLong();

        m_hashFlag = static_cast<DRTxnPartitionHashFlag>(m_taskInfo.readByte());

        m_txnLen = m_taskInfo.readInt();
        vassert(m_txnStart + m_txnLen <= m_logEnd);
        m_partitionHash = m_taskInfo.readInt();

        return true;
    }

    /**
     * @return the next record type read from the log
     */
    DRRecordType readRecordType() {
        DRRecordType type = static_cast<DRRecordType>(m_taskInfo.readByte());
        if (type == DR_RECORD_HASH_DELIMITER) {
            m_partitionHash = m_taskInfo.readInt();
            type = static_cast<DRRecordType>(m_taskInfo.readByte());
        }

        return type;
    }

    bool isReplicatedTableLog() {
        return m_hashFlag == TXN_PAR_HASH_REPLICATED;
    }

    ~BinaryLog() {
    }

private:
    BinaryLog(const char *log, int32_t logLength) :
            m_taskInfo(log + sizeof(int32_t), logLength) {
        initialize(logLength);
    }

    void initialize(int32_t logLength) {
        vassert(m_taskInfo.hasRemaining());
        m_logEnd = m_taskInfo.getRawPointer() + logLength;

        bool __attribute__ ((unused)) success = readNextTransaction();
        vassert(success);
    }

    static int32_t readRawInt(const char *log) {
        return ntohl(*reinterpret_cast<const int32_t*>(log));
    }

    ReferenceSerializeInputLE m_taskInfo;
    const char *m_txnStart;
    int64_t m_uniqueId;
    int64_t m_sequenceNumber;
    DRTxnPartitionHashFlag m_hashFlag;
    int32_t m_txnLen;
    int32_t m_partitionHash;
    const char *m_logEnd;
};

BinaryLogSink::BinaryLogSink() :
        m_drIgnoreConflicts(false),
        m_drCrcErrorIgnoreMax(-1),
        m_drCrcErrorFatal(true),
        m_crcErrorCount(0) {
}

// Shared success boolean used when applying binary logs for replicated table
bool s_replicatedApplySuccess;

int64_t BinaryLogSink::apply(const char *rawLogs,
        std::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine,
        int32_t remoteClusterId, int64_t localUniqueId) {
    int32_t logCount = BinaryLog::readRawInt(rawLogs);
    rawLogs += sizeof(int32_t);
    int64_t rowCount = 0;

    START_TIMER(apply);

    if (logCount == 1) {
        // Optimization for single log
        VOLT_DEBUG("Handling single binary log");
        boost::scoped_ptr<BinaryLog> log(BinaryLog::create(rawLogs));
        if (log != NULL) {
            rowCount = applyLog(log.get(), tables, pool, engine, remoteClusterId, localUniqueId);
        }
    } else {
        VOLT_DEBUG("Handling multiple binary logs %d", logCount);
        rowCount = applyMpTxn(rawLogs, logCount, tables, pool, engine, remoteClusterId, localUniqueId);
    }

    STOP_TIMER(apply, applyLogs, "applied %d logs", logCount);
    VOLT_DEBUG("Completed applying %d log(s) resulting in %jd rows", logCount, (intmax_t ) rowCount);
    return rowCount;
}

int64_t BinaryLogSink::applyMpTxn(const char *rawLogs, int32_t logCount,
        std::unordered_map<int64_t, PersistentTable*> &tables, Pool *pool, VoltDBEngine *engine,
        int32_t remoteClusterId, int64_t localUniqueId) {
    boost::scoped_array<boost::scoped_ptr<BinaryLog>> logs(new boost::scoped_ptr<BinaryLog>[logCount]);
    int64_t rowCount = 0;
    const char *position = rawLogs;
    int32_t specialLogCount = 0;

    // i is incremented at the end of the loop so logs can be easily skipped
    for (int i = 0; i < logCount;) {
        VOLT_DEBUG("Reading start transaction for log: %d", i);
        logs[i].reset(BinaryLog::create(position));
        if (logs[i] == NULL) {
            // Log had no contents so ignore it
            --logCount;
            continue;
        }

        if (i != 0) {
            if (logs[0]->m_uniqueId != logs[i]->m_uniqueId) {
                throwFatalException("Unique Id not the same for all sub transactions: unique ID %jd != %jd",
                        (intmax_t ) logs[0]->m_uniqueId, (intmax_t ) logs[i]->m_uniqueId);
            }
        }

        position = logs[i]->m_logEnd;

        // Handle the replicated table transaction up front to reduce the required site coordination
        if (logs[i]->isReplicatedTableLog()) {
            rowCount += applyReplicatedTxn(logs[i].get(), tables, pool, engine, remoteClusterId, localUniqueId);
            --logCount;
            continue;
        }

        if (logs[i]->m_hashFlag == TXN_PAR_HASH_SPECIAL) {
            // Sort all of the special logs to the start of the logs array
            if (specialLogCount != i) {
                logs[i].swap(logs[specialLogCount]);
            }
            ++specialLogCount;
        }

        ++i;
    }

    switch (logCount) {
    case 1:
        rowCount += applyLog(logs[0].get(), tables, pool, engine, remoteClusterId, localUniqueId);
        /* fallthrough */
    case 0:
        return rowCount;
    }

    vassert(UniqueId::isMpUniqueId(localUniqueId));

    VOLT_DEBUG("Applying MP binary log: log count: %d, unique ID: %jd, sequence number: %jd", logCount,
            (intmax_t) logs[0]->m_uniqueId, (intmax_t) logs[0]->m_sequenceNumber);

    /*
     * Iterate over all of the logs until they are depleted. If a TRUNCATE_TABLE is encountered stop processing
     * operations until the same truncate is encountered in all logs. Once all logs have encountered the truncate apply
     * the truncate once and continue processing the first log again.
     */
    int32_t completedLogs = 0;
    do {
        int32_t __attribute__((unused)) truncateCount = 0;
        int64_t truncateTableHandle = -1;
        std::string truncateTableName = std::string();

        for (int i = 0; i < logCount; ++i) {
            vassert(!logs[i]->isReplicatedTableLog());

            if (!logs[i]->m_taskInfo.hasRemaining() ||
                    // Skip processing log if a truncate has been encountered but this log does not have one
                    (truncateTableName.length() > 0 && logs[i]->m_hashFlag != TXN_PAR_HASH_SPECIAL)) {
                continue;
            }

            DRRecordType type;

            while ((type = logs[i]->readRecordType()) != DR_RECORD_END_TXN) {
                if (type == DR_RECORD_TRUNCATE_TABLE) {
                    ++truncateCount;
                    if (i == 0) {
                        vassert(truncateTableName.size() == 0);
                        truncateTableHandle = logs[i]->m_taskInfo.readLong();
                        truncateTableName = logs[i]->m_taskInfo.readTextString();
                    } else {
                        int64_t tempTableHandle = logs[i]->m_taskInfo.readLong();
                        std::string tempTableName = logs[i]->m_taskInfo.readTextString();

                        if (truncateTableHandle != tempTableHandle || truncateTableName.compare(tempTableName) != 0) {
                            throwFatalException("Table id or name not the same for all truncate transactions:"
                                    "table ID %jd != %jd or name '%s' != '%s' log %d", (intmax_t ) truncateTableHandle,
                                    (intmax_t ) tempTableHandle, truncateTableName.c_str(), tempTableName.c_str(), i);
                        }
                    }
                    break;
                }

                bool skipRow = !engine->isLocalSite(logs[i]->m_partitionHash);
                rowCount += applyRecord(logs[i].get(), type, tables, pool, engine, remoteClusterId, skipRow);
            }

            if (type == DR_RECORD_END_TXN) {
                vassert(truncateCount == 0);

                logs[i]->validateEndTxn(engine, &m_crcErrorCount, m_drCrcErrorIgnoreMax, m_drCrcErrorFatal);
                ++completedLogs;
            }
        }

        if (truncateTableName.size() > 0) {
            vassert(truncateCount == specialLogCount);
            truncateCount = 0;

            VOLT_DEBUG("Applying MP binary log truncate to %s", truncateTableName.c_str());
            truncateTable(tables, engine, truncateTableHandle, &truncateTableName);
            truncateTableName = std::string();
        }
    } while (completedLogs < logCount);

#ifndef NDEBUG
    for (int i = 0; i < logCount; ++i) {
        vassert(!logs[i]->m_taskInfo.hasRemaining());
    }
#endif

    return rowCount;
}

int64_t BinaryLogSink::applyLog(BinaryLog *log, std::unordered_map<int64_t, PersistentTable*> &tables,
        Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId, int64_t localUniqueId) {
    int64_t rowCount = 0;

    if (log->isReplicatedTableLog()) {
        return applyReplicatedTxn(log, tables, pool, engine, remoteClusterId, localUniqueId);
    }

    do {
        pool->purge();
        rowCount += applyTxn(log, tables, pool, engine, remoteClusterId, localUniqueId);
    } while (log->readNextTransaction());

    return rowCount;
}

int64_t BinaryLogSink::applyTxn(BinaryLog *log, std::unordered_map<int64_t, PersistentTable*> &tables,
                 Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId,
                 int64_t localUniqueId) {

    DRRecordType type;
    int64_t rowCount = 0;
    bool checkForSkip = !log->isReplicatedTableLog();
    bool canSkip = UniqueId::isMpUniqueId(localUniqueId);

    START_TIMER(timer);

    while ((type = log->readRecordType()) != DR_RECORD_END_TXN) {
        vassert(log->m_hashFlag != TXN_PAR_HASH_PLACEHOLDER);
        bool skipRow = false;
        if (checkForSkip && !engine->isLocalSite(log->m_partitionHash)) {
            if (canSkip) {
                skipRow = true;
            } else {
                throw SerializableEEException(
                        VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_TXN_MISPARTITIONED,
                        "Binary log txns were sent to the wrong partition");
            }
        }
        rowCount += applyRecord(log, type, tables, pool, engine, remoteClusterId, skipRow);
    }

    log->validateEndTxn(engine, &m_crcErrorCount, m_drCrcErrorIgnoreMax, m_drCrcErrorFatal);

    STOP_TIMER(timer, applyLogs, "applied %ld rows from %d uniqueId: %ld, sequenceNumber: %ld", rowCount,
            remoteClusterId, log->m_uniqueId, log->m_sequenceNumber);

    return rowCount;
}

int64_t BinaryLogSink::applyReplicatedTxn(BinaryLog *log, std::unordered_map<int64_t, PersistentTable*> &tables,
        Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId, int64_t localUniqueId) {
    ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(true, engine->isLowestSite(),
            []() { s_replicatedApplySuccess = false; });

    long rowCount = 0;
    if (possiblySynchronizedUseMpMemory.okToExecute()) {
        VOLT_TRACE("applyBinaryLogMP for replicated table");
        rowCount = applyTxn(log, tables, pool, engine, remoteClusterId, localUniqueId);
        s_replicatedApplySuccess = true;
    } else if (!s_replicatedApplySuccess) {
        const char* msg = "Replicated table apply binary log threw an unknown exception on other thread.";
        VOLT_DEBUG("%s", msg);
        throw SerializableEEException(
                VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE, msg);
    } else {
        VOLT_TRACE("Skipping applyBinaryLogMP for replicated table");
        log->skipRecordsAndValidateTxn(engine, &m_crcErrorCount, m_drCrcErrorIgnoreMax, m_drCrcErrorFatal);
    }

    vassert(!log->m_taskInfo.hasRemaining());

    return rowCount;
}

int64_t BinaryLogSink::applyRecord(
        BinaryLog *log, const DRRecordType type, std::unordered_map<int64_t, PersistentTable*> &tables,
        Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId, bool skipRow) {
    ReferenceSerializeInputLE *taskInfo = &log->m_taskInfo;
    int64_t uniqueId = log->m_uniqueId;
    int64_t sequenceNumber = log->m_sequenceNumber;

    START_TIMER(startTime);

#define STOP_TIMER_OP(msg, ...) STOP_TIMER(startTime, applyLogs,             \
        "Applying transaction from %d uniqueId: %ld, sequenceNumber: %ld " msg, \
        remoteClusterId, uniqueId, sequenceNumber, ##__VA_ARGS__)

    switch (type) {
    case DR_RECORD_INSERT: {
        int64_t tableHandle = taskInfo->readLong();
        int32_t rowLength = taskInfo->readInt();
        const char *rowData = reinterpret_cast<const char *>(taskInfo->getRawPointer(rowLength));
        if (skipRow) {
            break;
        }

        std::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwDRTableNotFoundException(tableHandle, "Unable to find table hash %jd while applying a binary log insert record",
                                          (intmax_t) tableHandle);
        }
        PersistentTable *table = tableIter->second;

        TableTuple tempTuple = table->tempTuple();

        ReferenceSerializeInputLE rowInput(rowData, rowLength);
        try {
            tempTuple.deserializeFromDR(rowInput, pool);
        } catch (SerializableEEException &e) {
            e.appendContextToMessage(" DR binary log insert on table " + table->name());
            throw;
        }
        try {
            table->insertPersistentTuple(tempTuple, true);
        } catch (ConstraintFailureException &e) {
            if (engine->getIsActiveActiveDREnabled()) {
                if (handleConflict(engine, table, pool, NULL, NULL, const_cast<TableTuple *>(e.getConflictTuple()),
                                   uniqueId, remoteClusterId, DR_RECORD_INSERT, NO_CONFLICT,
                                   CONFLICT_CONSTRAINT_VIOLATION, m_drIgnoreConflicts)) {
                    break;
                }
            }
            throw;
        }
        STOP_TIMER_OP("INSERT into %s of %s", table->name().c_str(), tempTuple.toJsonArray().c_str());
        break;
    }
    case DR_RECORD_DELETE: {
        int64_t tableHandle = taskInfo->readLong();
        int32_t rowLength = taskInfo->readInt();
        const char *rowData = reinterpret_cast<const char *>(taskInfo->getRawPointer(rowLength));
        if (skipRow) {
            break;
        }

        std::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwDRTableNotFoundException(tableHandle, "Unable to find table hash %jd while applying a binary log delete record",
                                         (intmax_t)tableHandle);
        }
        PersistentTable *table = tableIter->second;

        TableTuple tempTuple = table->tempTuple();

        ReferenceSerializeInputLE rowInput(rowData, rowLength);
        try {
            tempTuple.deserializeFromDR(rowInput, pool);
        } catch (SerializableEEException &e) {
            e.appendContextToMessage(" DR binary log delete on table " + table->name());
            throw;
        }

        TableTuple deleteTuple = table->lookupTupleForDR(tempTuple);
        if (deleteTuple.isNullTuple()) {
            if (engine->getIsActiveActiveDREnabled()) {
                if (handleConflict(engine, table, pool, NULL, &tempTuple, NULL, uniqueId, remoteClusterId, DR_RECORD_DELETE, CONFLICT_EXPECTED_ROW_MISSING, NO_CONFLICT, m_drIgnoreConflicts)) {
                    break;
                }
            }
            throwSerializableEEException("Unable to find tuple for deletion: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                             type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
        }

        // we still run in risk of having timestamp mismatch, need to check.
        if (engine->getIsActiveActiveDREnabled()) {
            NValue localHiddenColumn = deleteTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
            int64_t localTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(localHiddenColumn);
            NValue remoteHiddenColumn = tempTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
            int64_t remoteTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(remoteHiddenColumn);
            if (localTimestamp != remoteTimestamp) {
                // timestamp mismatch conflict
                if (handleConflict(engine, table, pool, &deleteTuple, &tempTuple, NULL, uniqueId, remoteClusterId, DR_RECORD_DELETE, CONFLICT_EXPECTED_ROW_MISMATCH, NO_CONFLICT, m_drIgnoreConflicts)) {
                    break;
                }
            }
        }

        table->deleteTuple(deleteTuple, true);
        STOP_TIMER_OP("DELETE from %s of %s", table->name().c_str(), tempTuple.toJsonArray().c_str());
        break;
    }
    case DR_RECORD_UPDATE: {
        int64_t tableHandle = taskInfo->readLong();
        int32_t oldRowLength = taskInfo->readInt();
        const char *oldRowData = reinterpret_cast<const char*>(taskInfo->getRawPointer(oldRowLength));
        int32_t newRowLength = taskInfo->readInt();
        const char *newRowData = reinterpret_cast<const char*>(taskInfo->getRawPointer(newRowLength));
        if (skipRow) {
            break;
        }

        std::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwDRTableNotFoundException(tableHandle, "Unable to find table hash %jd while applying a binary log update record",
                                         (intmax_t)tableHandle);
        }
        PersistentTable *table = tableIter->second;

        TableTuple tempTuple = table->tempTuple();

        ReferenceSerializeInputLE oldRowInput(oldRowData, oldRowLength);
        try {
            tempTuple.deserializeFromDR(oldRowInput, pool);
        } catch (SerializableEEException &e) {
            e.appendContextToMessage(" DR binary log update (old tuple) on table " + table->name());
            throw;
        }

        // create the expected tuple
        TableTuple expectedTuple(table->schema());
        boost::shared_array<char> expectedData = boost::shared_array<char>(new char[tempTuple.tupleLength()]);
        expectedTuple.move(expectedData.get());
        expectedTuple.copyForPersistentInsert(tempTuple, pool);

        ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
        try {
            tempTuple.deserializeFromDR(newRowInput, pool);
        } catch (SerializableEEException &e) {
            e.appendContextToMessage(" DR binary log update (new tuple) on table " + table->name());
            throw;
        }

        TableTuple oldTuple = table->lookupTupleForDR(expectedTuple);
        if (oldTuple.isNullTuple()) {
            if (engine->getIsActiveActiveDREnabled()) {
                if (handleConflict(engine, table, pool, NULL, &expectedTuple,
                                   &tempTuple, uniqueId, remoteClusterId,
                                   DR_RECORD_UPDATE, CONFLICT_EXPECTED_ROW_MISSING,
                                   NO_CONFLICT, m_drIgnoreConflicts)) {
                    break;
                }
            }
            throwSerializableEEException("Unable to find tuple for update: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                     type, (intmax_t)sequenceNumber, (intmax_t)uniqueId, tempTuple.debug(table->name()).c_str());
        }

        // Timestamp mismatch conflict
        if (engine->getIsActiveActiveDREnabled()) {
            NValue localHiddenColumn = oldTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
            int64_t localTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(localHiddenColumn);
            NValue remoteHiddenColumn = expectedTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
            int64_t remoteTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(remoteHiddenColumn);
            if (localTimestamp != remoteTimestamp) {
                if (handleConflict(engine, table, pool, &oldTuple, &expectedTuple,
                                   &tempTuple, uniqueId, remoteClusterId,
                                   DR_RECORD_UPDATE, CONFLICT_EXPECTED_ROW_MISMATCH,
                                   NO_CONFLICT, m_drIgnoreConflicts)) {
                    break;
                }
            }
        }

        try {
            table->updateTupleWithSpecificIndexes(oldTuple, tempTuple, table->allIndexes(), true, false);
        } catch (ConstraintFailureException &e) {
            if (engine->getIsActiveActiveDREnabled()) {
                if (handleConflict(engine, table, pool, NULL, e.getOriginalTuple(),
                                   const_cast<TableTuple *>(e.getConflictTuple()),
                                   uniqueId, remoteClusterId, DR_RECORD_UPDATE,
                                   NO_CONFLICT, CONFLICT_CONSTRAINT_VIOLATION, m_drIgnoreConflicts)) {
                    break;
                }
            }
            throw;
        }
        STOP_TIMER_OP("UPDATE in %s of %s to %s", table->name().c_str(), oldTuple.toJsonArray().c_str(), tempTuple.toJsonArray().c_str());
        break;
    }
    case DR_RECORD_DELETE_BY_INDEX: {
        throwSerializableEEException("Delete by index is not supported for DR");
    }
    case DR_RECORD_UPDATE_BY_INDEX: {
        throwSerializableEEException("Update by index is not supported for DR");
    }
    case DR_RECORD_TRUNCATE_TABLE: {
        int64_t tableHandle = taskInfo->readLong();
        std::string tableName = taskInfo->readTextString();

        truncateTable(tables, engine, tableHandle, &tableName);
        STOP_TIMER_OP("TRUNCATE TABLE %s", tableName.c_str());

        break;
    }
    case DR_RECORD_BEGIN_TXN: {
        throwFatalException("Unexpected BEGIN_TXN before END_TXN");
        break;
    }
    default:
        throwFatalException("Unrecognized DR record type %d", type);
        break;
    }
    return static_cast<int64_t>(rowCostForDRRecord(type));
}

}
