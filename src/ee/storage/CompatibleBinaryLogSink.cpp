/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "CompatibleBinaryLogSink.h"

#include "ConstraintFailureException.h"
#include "persistenttable.h"
#include "streamedtable.h"
#include "tablefactory.h"
#include "temptable.h"

#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "common/UniqueId.hpp"
#include "indexes/tableindex.h"

#include "catalog/database.h"

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include <crc/crc32c.h>

#include <string>

// IMPORTANT: DON'T CHANGE THIS FILE, THIS IS A FIXED VERSION OF DR STREAM ONLY FOR COMPATIBILITY MODE.

namespace voltdb {

const static std::string EXISTING_TABLE = "existing_table";
const static std::string EXPECTED_TABLE = "expected_table";
const static std::string NEW_TABLE = "new_table";

// column indices of DR conflict export table
const static int DR_ROW_TYPE_COLUMN_INDEX = 0;
const static int DR_LOG_ACTION_COLUMN_INDEX = 1;
const static int DR_CONFLICT_COLUMN_INDEX = 2;
const static int DR_CONFLICTS_ON_PK_COLUMN_INDEX = 3;
const static int DR_ACTION_DECISION_COLUMN_INDEX = 4;
const static int DR_CLUSTER_ID_COLUMN_INDEX = 5;
const static int DR_TIMESTAMP_COLUMN_INDEX = 6;
const static int DR_DIVERGENCE_COLUMN_INDEX = 7;
const static int DR_TABLE_NAME_COLUMN_INDEX = 8;
const static int DR_TUPLE_COLUMN_INDEX = 9;

const static int DECISION_BIT = 1;
const static int RESOLVED_BIT = 1 << 1;

class CachedIndexKeyTuple {
public:
    CachedIndexKeyTuple() : m_tuple(), m_cachedIndexCrc(0), m_storageSize(0), m_tupleStorage() {}

    TableTuple &tuple(PersistentTable *table, uint32_t indexCrc) {
        if (m_storageSize > 0 && indexCrc == m_cachedIndexCrc) {
            return m_tuple;
        }
        std::pair<const TableIndex*, uint32_t> index = table->getUniqueIndexForDR();
        if (!index.first || indexCrc != index.second) {
            throwSerializableEEException("Unable to find unique index %u while applying a binary log record",
                                         indexCrc);
        }
        const TupleSchema* schema = index.first->getKeySchema();
        size_t tupleLength = schema->tupleLength() + TUPLE_HEADER_SIZE;
        if (tupleLength > m_storageSize) {
            m_tupleStorage.reset(new char[tupleLength]);
            m_storageSize = tupleLength;
        }
        m_tuple.setSchema(schema);
        m_tuple.move(m_tupleStorage.get());
        m_cachedIndexCrc = index.second;
        return m_tuple;
    }
private:
    TableTuple m_tuple;
    uint32_t m_cachedIndexCrc;
    size_t m_storageSize;
    boost::scoped_array<char> m_tupleStorage;
};

// a c++ style way to limit access from outside this file
namespace {

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

void exportTuples(Table *exportTable, Table *metaTable, Table *tupleTable) {
    TableTuple tempMetaTuple(exportTable->schema());
    TableTuple tempTupleTuple(tupleTable->schema());
    TableIterator metaIter = metaTable->iterator();
    TableIterator tupleIter = tupleTable->iterator();
    while (metaIter.next(tempMetaTuple) && tupleIter.next(tempTupleTuple)) {
        tempMetaTuple.setNValue(DR_TUPLE_COLUMN_INDEX, ValueFactory::getTempStringValue(tempTupleTuple.toJsonArray()));
        exportTable->insertTuple(tempMetaTuple);
    }
}

typedef std::pair<boost::shared_ptr<TableTuple>, bool>  LabeledTableTuple;

/**
   * Find all rows in a @table that conflict with the @searchTuple (unique key violation) except the @expectedTuple
   * All conflicting rows are put into @conflictRows.
   */
void findConflictTuple(PersistentTable *table, const TableTuple *existingTuple, const TableTuple *searchTuple, const TableTuple *expectedTuple, std::vector< LabeledTableTuple > &conflictRows) {
    boost::unordered_set<char*> redundancyFilter;
    BOOST_FOREACH(TableIndex* index, table->allIndexes()) {
        if (index->isUniqueIndex()) {
            IndexCursor cursor(index->getTupleSchema());
            if (index->moveToKeyByTuple(searchTuple, cursor)) {
                TableTuple conflictTuple = index->nextValueAtKey(cursor);
                if (expectedTuple) {
                    if (expectedTuple->equals(conflictTuple)) {
                        // exclude the expected tuple in update
                        continue;
                    } else if (existingTuple && existingTuple->equals(conflictTuple)) {
                        // in update this row was already listed in existingTableForDelete,
                        // don't include it in existingTableForInsert.
                        continue;
                    }
                }
                if (redundancyFilter.find(conflictTuple.address()) != redundancyFilter.end()) {
                    // skip the conflict tuples that are already found
                    continue;
                } else {
                    conflictRows.push_back(std::make_pair(boost::shared_ptr<TableTuple>(new TableTuple(conflictTuple)), table->primaryKeyIndex() == index ? true : false));
                    redundancyFilter.insert(conflictTuple.address());
                }
            }
        }
    }
}

/**
 * create conflict export tuple from the conflict tuple
 */
void createConflictExportTuple(TempTable *outputMetaTable, TempTable *outputTupleTable, PersistentTable *drTable, Pool *pool, const TableTuple *tupleToBeWrote,
        DRConflictOnPK conflictOnPKType, DRRecordType actionType, DRConflictType conflictType, DRConflictRowType rowType) {
    TableTuple tempMetaTuple = outputMetaTable->tempTuple();
    NValue hiddenValue = tupleToBeWrote->getHiddenNValue(drTable->getDRTimestampColumnIndex());
    tempMetaTuple.setNValue(DR_ROW_TYPE_COLUMN_INDEX, ValueFactory::getTempStringValue(DRConflictRowTypeStr(rowType)));
    tempMetaTuple.setNValue(DR_LOG_ACTION_COLUMN_INDEX, ValueFactory::getTempStringValue(DRRecordTypeStr(actionType)));
    tempMetaTuple.setNValue(DR_CONFLICT_COLUMN_INDEX, ValueFactory::getTempStringValue(DRConflictTypeStr(conflictType)));
    tempMetaTuple.setNValue(DR_CONFLICTS_ON_PK_COLUMN_INDEX, ValueFactory::getTinyIntValue(conflictOnPKType));
    tempMetaTuple.setNValue(DR_ACTION_DECISION_COLUMN_INDEX, ValueFactory::getTempStringValue(DRDecisionStr(REJECT)));
    tempMetaTuple.setNValue(DR_CLUSTER_ID_COLUMN_INDEX, ValueFactory::getTinyIntValue((ExecutorContext::getClusterIdFromHiddenNValue(hiddenValue))));
    tempMetaTuple.setNValue(DR_TIMESTAMP_COLUMN_INDEX, ValueFactory::getBigIntValue(ExecutorContext::getDRTimestampFromHiddenNValue(hiddenValue)));
    tempMetaTuple.setNValue(DR_DIVERGENCE_COLUMN_INDEX, ValueFactory::getTempStringValue(DRDivergenceStr(NOT_DIVERGE)));
    tempMetaTuple.setNValue(DR_TABLE_NAME_COLUMN_INDEX, ValueFactory::getTempStringValue(drTable->name()));
    tempMetaTuple.setNValue(DR_TUPLE_COLUMN_INDEX, ValueFactory::getNullStringValue());
    // Must have to deep copy non-inlined data, because tempTuple may be overwritten by following call of this function.
    outputMetaTable->insertTempTupleDeepCopy(tempMetaTuple, pool);

    outputTupleTable->insertTempTupleDeepCopy(*tupleToBeWrote, pool);
}

// iterate all tables and push them into export table
void exportDRConflict(Table *exportTable, bool applyRemoteChange, bool resolved,
        TempTable *existingMetaTableForDelete, TempTable *existingTupleTableForDelete,
        TempTable *expectedMetaTableForDelete, TempTable *expectedTupleTableForDelete,
        TempTable *existingMetaTableForInsert, TempTable *existingTupleTableForInsert,
        TempTable *newMetaTableForInsert, TempTable *newTupleTableForInsert) {
    assert(exportTable != NULL);
    assert((existingMetaTableForDelete == NULL && existingTupleTableForDelete == NULL) ||
           (existingMetaTableForDelete != NULL && existingTupleTableForDelete != NULL));
    assert((expectedMetaTableForDelete == NULL && expectedTupleTableForDelete == NULL) ||
           (expectedMetaTableForDelete != NULL && expectedTupleTableForDelete != NULL));
    assert((existingMetaTableForInsert == NULL && existingTupleTableForInsert == NULL) ||
           (existingMetaTableForInsert != NULL && existingTupleTableForInsert != NULL));
    assert((newMetaTableForInsert == NULL && newTupleTableForInsert == NULL) ||
           (newMetaTableForInsert != NULL && newTupleTableForInsert != NULL));

    if (existingMetaTableForDelete) {
        exportTuples(exportTable, existingMetaTableForDelete, existingTupleTableForDelete);
    }

    if (expectedMetaTableForDelete) {
        exportTuples(exportTable, expectedMetaTableForDelete, expectedTupleTableForDelete);
    }

    if (existingMetaTableForInsert) {
        exportTuples(exportTable, existingMetaTableForInsert, existingTupleTableForInsert);
    }

    if (newMetaTableForInsert) {
        exportTuples(exportTable, newMetaTableForInsert, newTupleTableForInsert);
    }
}

void validateChecksum(uint32_t checksum, const char *start, const char *end) {
    uint32_t recalculatedCRC = vdbcrc::crc32cInit();
    recalculatedCRC = vdbcrc::crc32c( recalculatedCRC, start, (end - 4) - start);
    recalculatedCRC = vdbcrc::crc32cFinish(recalculatedCRC);

    if (recalculatedCRC != checksum) {
        throwFatalException("CRC mismatch of DR log data %d and %d", checksum, recalculatedCRC);
    }
}

bool handleConflict(VoltDBEngine *engine, PersistentTable *drTable, Pool *pool, TableTuple *existingTuple, const TableTuple *expectedTuple, TableTuple *newTuple,
        int64_t uniqueId, int32_t remoteClusterId, DRRecordType actionType, DRConflictType deleteConflict, DRConflictType insertConflict) {
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
    if (deleteConflict != NO_CONFLICT) {
        existingMetaTableForDelete.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, conflictExportTable, NULL));
        existingTupleTableForDelete.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, drTable, NULL));
        if (existingTuple) {
            createConflictExportTuple(existingMetaTableForDelete.get(), existingTupleTableForDelete.get(),
                    drTable, pool, existingTuple, NOT_CONFLICT_ON_PK, actionType, deleteConflict, EXISTING_ROW);
        }

    }
    if (expectedTuple) {
        expectedMetaTableForDelete.reset(TableFactory::buildCopiedTempTable(EXPECTED_TABLE, conflictExportTable, NULL));
        expectedTupleTableForDelete.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, drTable, NULL));
        createConflictExportTuple(expectedMetaTableForDelete.get(), expectedTupleTableForDelete.get(),
                drTable, pool, expectedTuple, NOT_CONFLICT_ON_PK, actionType, deleteConflict, EXPECTED_ROW);
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
        existingMetaTableForInsert.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, conflictExportTable, NULL));
        existingTupleTableForInsert.reset(TableFactory::buildCopiedTempTable(EXISTING_TABLE, drTable, NULL));
        if (existingRows.size() > 0) {
            BOOST_FOREACH(LabeledTableTuple labeledTuple, existingRows) {
                createConflictExportTuple(existingMetaTableForInsert.get(), existingTupleTableForInsert.get(), drTable,
                        pool, labeledTuple.first.get(), labeledTuple.second ? CONFLICT_ON_PK : NOT_CONFLICT_ON_PK,
                        actionType, insertConflict, EXISTING_ROW);
            }
        }
    }

    if (newTuple) {
        newMetaTableForInsert.reset(TableFactory::buildCopiedTempTable(NEW_TABLE, conflictExportTable, NULL));
        newTupleTableForInsert.reset(TableFactory::buildCopiedTempTable(NEW_TABLE, drTable, NULL));
        createConflictExportTuple(newMetaTableForInsert.get(), newTupleTableForInsert.get(),
                                  drTable, pool, newTuple, NOT_CONFLICT_ON_PK, actionType, insertConflict, NEW_ROW);
    }

    int retval = ExecutorContext::getExecutorContext()->getTopend()->reportDRConflict(static_cast<int32_t>(UniqueId::pid(uniqueId)),
                                                                                      remoteClusterId,
                                                                                      UniqueId::timestampSinceUnixEpoch(uniqueId),
                                                                                      drTable->name(),
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
    assert(resolved || !applyRemoteChange);

    if (existingMetaTableForDelete) {
        setConflictOutcome(existingMetaTableForDelete, applyRemoteChange, resolved);
    }
    if (expectedMetaTableForDelete) {
        setConflictOutcome(expectedMetaTableForDelete, applyRemoteChange, resolved);
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
            drTable->insertPersistentTuple(*newTuple, true, true);
        }
    }

    // For replicated table, pick partition 0 to export the conflicts.
    if (!drTable->isReplicatedTable() || engine->getPartitionId() == 0) {
        exportDRConflict(conflictExportTable, applyRemoteChange, resolved,
                existingMetaTableForDelete.get(), existingTupleTableForDelete.get(),
                expectedMetaTableForDelete.get(), expectedTupleTableForDelete.get(),
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
} // end of anonymous namespace

int64_t CompatibleBinaryLogSink::apply(ReferenceSerializeInputLE *taskInfo,
                                       boost::unordered_map<int64_t, PersistentTable*> &tables,
                                       Pool *pool, VoltDBEngine *engine, int32_t remoteClusterId,
                                       const char *recordStart, int64_t *uniqueId,
                                       int64_t *sequenceNumber) {
    CachedIndexKeyTuple indexKeyTuple;
    const DRRecordType type = static_cast<DRRecordType>(taskInfo->readByte());
    size_t rowCount = rowCostForDRRecord(type);

    switch (type) {
    case DR_RECORD_INSERT: {
        int64_t tableHandle = taskInfo->readLong();
        int32_t rowLength = taskInfo->readInt();
        const char *rowData = reinterpret_cast<const char *>(taskInfo->getRawPointer(rowLength));
        uint32_t checksum = taskInfo->readInt();
        validateChecksum(checksum, recordStart, taskInfo->getRawPointer());

        boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwSerializableEEException("Unable to find table hash %jd while applying a binary log insert record",
                                         (intmax_t)tableHandle);
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
            table->insertPersistentTuple(tempTuple, true, true);
        } catch (ConstraintFailureException &e) {
            if (engine->getIsActiveActiveDREnabled()) {
                if (handleConflict(engine, table, pool, NULL, NULL, const_cast<TableTuple *>(e.getConflictTuple()), *uniqueId, remoteClusterId, DR_RECORD_INSERT, NO_CONFLICT, CONFLICT_CONSTRAINT_VIOLATION)) {
                    break;
                }
            }
            throw;
        }
        break;
    }
    case DR_RECORD_DELETE: {
        int64_t tableHandle = taskInfo->readLong();
        int32_t rowLength = taskInfo->readInt();
        const char *rowData = reinterpret_cast<const char *>(taskInfo->getRawPointer(rowLength));
        uint32_t checksum = taskInfo->readInt();
        validateChecksum(checksum, recordStart, taskInfo->getRawPointer());

        boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwSerializableEEException("Unable to find table hash %jd while applying a binary log delete record",
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
                if (handleConflict(engine, table, pool, NULL, &tempTuple, NULL, *uniqueId, remoteClusterId, DR_RECORD_DELETE, CONFLICT_EXPECTED_ROW_MISSING, NO_CONFLICT)) {
                    break;
                }
            }
            throwSerializableEEException("Unable to find tuple for deletion: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                             type, (intmax_t)*sequenceNumber, (intmax_t)*uniqueId, tempTuple.debug(table->name()).c_str());
        }

        // we still run in risk of having timestamp mismatch, need to check.
        if (engine->getIsActiveActiveDREnabled()) {
            NValue localHiddenColumn = deleteTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
            int64_t localTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(localHiddenColumn);
            NValue remoteHiddenColumn = tempTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
            int64_t remoteTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(remoteHiddenColumn);
            if (localTimestamp != remoteTimestamp) {
                // timestamp mismatch conflict
                if (handleConflict(engine, table, pool, &deleteTuple, &tempTuple, NULL, *uniqueId, remoteClusterId, DR_RECORD_DELETE, CONFLICT_EXPECTED_ROW_MISMATCH, NO_CONFLICT)) {
                    break;
                }
            }
        }

        table->deleteTuple(deleteTuple, true);
        break;
    }
    case DR_RECORD_UPDATE: {
        int64_t tableHandle = taskInfo->readLong();
        int32_t oldRowLength = taskInfo->readInt();
        const char *oldRowData = reinterpret_cast<const char*>(taskInfo->getRawPointer(oldRowLength));
        int32_t newRowLength = taskInfo->readInt();
        const char *newRowData = reinterpret_cast<const char*>(taskInfo->getRawPointer(newRowLength));
        uint32_t checksum = taskInfo->readInt();
        validateChecksum(checksum, recordStart, taskInfo->getRawPointer());

        boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwSerializableEEException("Unable to find table hash %jd while applying a binary log update record",
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
                if (handleConflict(engine, table, pool, NULL, &expectedTuple, &tempTuple,
                                   *uniqueId, remoteClusterId, DR_RECORD_UPDATE,
                                   CONFLICT_EXPECTED_ROW_MISSING, NO_CONFLICT)) {
                    break;
                }
            }
            throwSerializableEEException("Unable to find tuple for update: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                     type, (intmax_t)*sequenceNumber, (intmax_t)*uniqueId, tempTuple.debug(table->name()).c_str());
        }

        // Timestamp mismatch conflict
        if (engine->getIsActiveActiveDREnabled()) {
            NValue localHiddenColumn = oldTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
            int64_t localTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(localHiddenColumn);
            NValue remoteHiddenColumn = expectedTuple.getHiddenNValue(table->getDRTimestampColumnIndex());
            int64_t remoteTimestamp = ExecutorContext::getDRTimestampFromHiddenNValue(remoteHiddenColumn);
            if (localTimestamp != remoteTimestamp) {
                if (handleConflict(engine, table, pool, &oldTuple, &expectedTuple,
                                   &tempTuple, *uniqueId, remoteClusterId,
                                   DR_RECORD_UPDATE, CONFLICT_EXPECTED_ROW_MISMATCH,
                                   NO_CONFLICT)) {
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
                                   *uniqueId, remoteClusterId, DR_RECORD_UPDATE,
                                   NO_CONFLICT, CONFLICT_CONSTRAINT_VIOLATION)) {
                    break;
                }
            }
            throw;
        }
        break;
    }
    case DR_RECORD_DELETE_BY_INDEX: {
        int64_t tableHandle = taskInfo->readLong();
        int32_t rowKeyLength = taskInfo->readInt();
        uint32_t indexCrc = taskInfo->readInt();
        const char *rowKeyData = reinterpret_cast<const char *>(taskInfo->getRawPointer(rowKeyLength));
        uint32_t checksum = taskInfo->readInt();
        validateChecksum(checksum, recordStart, taskInfo->getRawPointer());

        boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwSerializableEEException("Unable to find table hash %jd while applying a binary log delete record",
                                         (intmax_t)tableHandle);
        }
        PersistentTable *table = tableIter->second;

        TableTuple tempTuple = indexKeyTuple.tuple(table, indexCrc);

        ReferenceSerializeInputLE rowInput(rowKeyData, rowKeyLength);
        try {
            tempTuple.deserializeFromDR(rowInput, pool);
        } catch (SerializableEEException &e) {
            e.appendContextToMessage(" DR binary log delete by index on table " + table->name());
            throw;
        }

        const TableIndex* index = table->getUniqueIndexForDR().first;
        IndexCursor indexCursor(index->getTupleSchema());
        index->moveToKey(&tempTuple, indexCursor);
        TableTuple deleteTuple = index->nextValueAtKey(indexCursor);
        if (deleteTuple.isNullTuple()) {
            throwSerializableEEException("Unable to find tuple for deletion: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                         type, (intmax_t)*sequenceNumber, (intmax_t)*uniqueId, tempTuple.debug(table->name()).c_str());
        }

        table->deleteTuple(deleteTuple, true);
        break;
    }
    case DR_RECORD_UPDATE_BY_INDEX: {
        int64_t tableHandle = taskInfo->readLong();
        int32_t oldRowKeyLength = taskInfo->readInt();
        uint32_t oldKeyIndexCrc = taskInfo->readInt();
        const char *oldRowKeyData = reinterpret_cast<const char*>(taskInfo->getRawPointer(oldRowKeyLength));
        int32_t newRowLength = taskInfo->readInt();
        const char *newRowData = reinterpret_cast<const char*>(taskInfo->getRawPointer(newRowLength));
        uint32_t checksum = taskInfo->readInt();
        validateChecksum(checksum, recordStart, taskInfo->getRawPointer());

        boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwSerializableEEException("Unable to find table hash %jd while applying a binary log update record",
                                         (intmax_t)tableHandle);
        }
        PersistentTable *table = tableIter->second;

        TableTuple tempTuple = indexKeyTuple.tuple(table, oldKeyIndexCrc);

        ReferenceSerializeInputLE oldRowInput(oldRowKeyData, oldRowKeyLength);
        try {
            tempTuple.deserializeFromDR(oldRowInput, pool);
        } catch (SerializableEEException &e) {
            e.appendContextToMessage(" DR binary log update by index (old tuple) on table " + table->name());
            throw;
        }

        const TableIndex* index = table->getUniqueIndexForDR().first;
        IndexCursor indexCursor(index->getTupleSchema());
        index->moveToKey(&tempTuple, indexCursor);
        TableTuple oldTuple = index->nextValueAtKey(indexCursor);
        if (oldTuple.isNullTuple()) {
            throwSerializableEEException("Unable to find tuple for update: binary log type (%d), DR ID (%jd), unique ID (%jd), tuple %s\n",
                                        type, (intmax_t)*sequenceNumber, (intmax_t)*uniqueId, tempTuple.debug(table->name()).c_str());
        }

        tempTuple = table->tempTuple();
        ReferenceSerializeInputLE newRowInput(newRowData, newRowLength);
        try {
            tempTuple.deserializeFromDR(newRowInput, pool);
        } catch (SerializableEEException &e) {
            e.appendContextToMessage(" DR binary log update by index (new tuple) on table " + table->name());
            throw;
        }

        table->updateTupleWithSpecificIndexes(oldTuple, tempTuple, table->allIndexes(), true, false);
        break;
    }
    case DR_RECORD_BEGIN_TXN: {
        *uniqueId = taskInfo->readLong();
        int64_t tempSequenceNumber = taskInfo->readLong();
        if (*sequenceNumber >= 0) {
            if (tempSequenceNumber < *sequenceNumber) {
                throwFatalException("Found out of order sequencing inside a binary log segment. Expected %jd but found %jd",
                                    (intmax_t)(*sequenceNumber + 1), (intmax_t)tempSequenceNumber);
            } else if (tempSequenceNumber == *sequenceNumber) {
                throwFatalException("Found duplicate transaction %jd in a binary log segment",
                                    (intmax_t)tempSequenceNumber);
            } else if (tempSequenceNumber > *sequenceNumber + 1) {
                throwFatalException("Found sequencing gap inside a binary log segment. Expected %jd but found %jd",
                                    (intmax_t)(*sequenceNumber + 1), (intmax_t)tempSequenceNumber);
            }
        }
        *sequenceNumber = tempSequenceNumber;
        uint32_t checksum = taskInfo->readInt();
        validateChecksum(checksum, recordStart, taskInfo->getRawPointer());
        break;
    }
    case DR_RECORD_END_TXN: {
        int64_t tempSequenceNumber = taskInfo->readLong();
        if (tempSequenceNumber != *sequenceNumber) {
            throwFatalException("Closing the wrong transaction inside a binary log segment. Expected %jd but found %jd",
                                (intmax_t)*sequenceNumber, (intmax_t)tempSequenceNumber);
        }
        uint32_t checksum = taskInfo->readInt();
        validateChecksum(checksum, recordStart, taskInfo->getRawPointer());
        break;
    }
    case DR_RECORD_TRUNCATE_TABLE: {
        int64_t tableHandle = taskInfo->readLong();
        std::string tableName = taskInfo->readTextString();

        uint32_t checksum = taskInfo->readInt();
        validateChecksum(checksum, recordStart, taskInfo->getRawPointer());

        boost::unordered_map<int64_t, PersistentTable*>::iterator tableIter = tables.find(tableHandle);
        if (tableIter == tables.end()) {
            throwSerializableEEException("Unable to find table %s hash %jd while applying binary log for truncate record",
                                         tableName.c_str(), (intmax_t)tableHandle);
        }

        PersistentTable *table = tableIter->second;

        table->truncateTable(engine, true);

        break;
    }
    default:
        throwFatalException("Unrecognized DR record type %d", type);
        break;
    }
    return static_cast<int64_t>(rowCount);
}

}
