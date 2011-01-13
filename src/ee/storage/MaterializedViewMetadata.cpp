/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#include <cassert>
#include <cstdio>
#include "boost/shared_array.hpp"
#include "common/types.h"
#include "common/FatalException.hpp"
#include "catalog/catalog.h"
#include "catalog/columnref.h"
#include "catalog/column.h"
#include "catalog/table.h"
#include "catalog/materializedviewinfo.h"
#include "expressions/abstractexpression.h"
#include "indexes/tableindex.h"
#include "storage/persistenttable.h"
#include "storage/MaterializedViewMetadata.h"

namespace voltdb {

MaterializedViewMetadata::MaterializedViewMetadata(
        PersistentTable *srcTable, PersistentTable *destTable, catalog::MaterializedViewInfo *metadata)
        : m_target(destTable), m_filterPredicate(NULL) {

    // try to load the predicate from the catalog view
    parsePredicate(metadata);

    // set up the group by columns from the catalog info
    m_groupByColumnCount = metadata->groupbycols().size();
    m_groupByColumns = new int32_t[m_groupByColumnCount];
    std::map<std::string, catalog::ColumnRef*>::const_iterator colRefIterator;
    for (colRefIterator = metadata->groupbycols().begin();
         colRefIterator != metadata->groupbycols().end();
         colRefIterator++)
    {
        int32_t grouping_order_offset = colRefIterator->second->index();
        m_groupByColumns[grouping_order_offset] = colRefIterator->second->column()->index();
    }

    // set up the mapping from input col to output col
    m_outputColumnCount = metadata->dest()->columns().size();
    m_outputColumnSrcTableIndexes = new int32_t[m_outputColumnCount];
    m_outputColumnAggTypes = new ExpressionType[m_outputColumnCount];
    std::map<std::string, catalog::Column*>::const_iterator colIterator;
    // iterate the source table
    for (colIterator = metadata->dest()->columns().begin(); colIterator != metadata->dest()->columns().end(); colIterator++) {
        const catalog::Column *destCol = colIterator->second;
        int destIndex = destCol->index();

        const catalog::Column *srcCol = destCol->matviewsource();

        if (srcCol) {
            m_outputColumnSrcTableIndexes[destIndex] = srcCol->index();
            m_outputColumnAggTypes[destIndex] = static_cast<ExpressionType>(destCol->aggregatetype());
        }
        else {
            m_outputColumnSrcTableIndexes[destIndex] = -1;
            m_outputColumnAggTypes[destIndex] = EXPRESSION_TYPE_INVALID;
        }
    }

    m_index = m_target->primaryKeyIndex();
    m_searchKey = TableTuple(m_index->getKeySchema());
    m_searchKeyBackingStore = new char[m_index->getKeySchema()->tupleLength() + 1];
    memset(m_searchKeyBackingStore, 0, m_index->getKeySchema()->tupleLength() + 1);
    m_searchKey.move(m_searchKeyBackingStore);

    m_existingTuple = TableTuple(m_target->schema());

    m_updatedTuple = TableTuple(m_target->schema());
    m_updatedTupleBackingStore = new char[m_target->schema()->tupleLength() + 1];
    memset(m_updatedTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);
    m_updatedTuple.move(m_updatedTupleBackingStore);

    m_emptyTuple = TableTuple(m_target->schema());
    m_emptyTupleBackingStore = new char[m_target->schema()->tupleLength() + 1];
    memset(m_emptyTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);
    m_emptyTuple.move(m_emptyTupleBackingStore);
}

MaterializedViewMetadata::~MaterializedViewMetadata() {
    delete[] m_searchKeyBackingStore;
    delete[] m_updatedTupleBackingStore;
    delete[] m_emptyTupleBackingStore;
    delete[] m_groupByColumns;
    delete[] m_outputColumnSrcTableIndexes;
    delete[] m_outputColumnAggTypes;
    delete m_filterPredicate;
}

void MaterializedViewMetadata::parsePredicate(catalog::MaterializedViewInfo *metadata) {
    std::string hexString = metadata->predicate();
    if (hexString.size() == 0)
        return;

    assert (hexString.length() % 2 == 0);
    int bufferLength = (int)hexString.size() / 2 + 1;
    boost::shared_array<char> buffer(new char[bufferLength]);
    catalog::Catalog::hexDecodeString(hexString, buffer.get());
    std::string bufferString(buffer.get());
    json_spirit::Value predicateValue;
    json_spirit::read( bufferString, predicateValue );

    if (!(predicateValue == json_spirit::Value::null)) {
        json_spirit::Object predicateObject = predicateValue.get_obj();
        m_filterPredicate = AbstractExpression::buildExpressionTree(predicateObject);
    }
}

void MaterializedViewMetadata::processTupleInsert(TableTuple &newTuple) {
    // don't change the view if this tuple doesn't match the predicate
    if (m_filterPredicate
        && (m_filterPredicate->eval(&newTuple, NULL).isFalse()))
        return;

    bool exists = findExistingTuple(newTuple);
    if (!exists) {
        // create a blank tuple
        m_existingTuple.move(m_emptyTupleBackingStore);
    }

    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);

    int colindex = 0;
    // set up the first n columns, based on group-by columns
    for (colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        // note that if the tuple is in the mv's target table,
        // tuple values should be pulled from the existing tuple in
        // that table. This works around a memory ownership issue
        // related to out-of-line strings.
        if (exists) {
            m_updatedTuple.setNValue(colindex,
                                 m_existingTuple.getNValue(colindex));
        }
        else {
            m_updatedTuple.setNValue(colindex,
                                 newTuple.getNValue(m_groupByColumns[colindex]));
        }
    }

    // set up the next column, which is a count
    m_updatedTuple.setNValue(colindex,
                             m_existingTuple.getNValue(colindex).op_increment());
    colindex++;

    // set values for the other columns
    for (int i = colindex; i < m_outputColumnCount; i++) {
        NValue newValue = newTuple.getNValue(m_outputColumnSrcTableIndexes[i]);
        NValue existingValue = m_existingTuple.getNValue(i);

        if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_SUM) {
            m_updatedTuple.setNValue(i, newValue.op_add(existingValue));
        }
        else if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_COUNT) {
            m_updatedTuple.setNValue(i, existingValue.op_increment());
        }
        else {
            char message[128];
            snprintf(message, 128, "Error in materialized view table update for"
                    " col %d. Expression type %d", i, m_outputColumnAggTypes[i]);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          message);
        }
    }

    // update or insert the row
    if (exists) {
        // shouldn't need to update indexes as this shouldn't ever change the
        // key
        m_target->updateTuple(m_updatedTuple, m_existingTuple, false);
    }
    else {
        m_target->insertTuple(m_updatedTuple);
    }
}

void MaterializedViewMetadata::processTupleUpdate(TableTuple &oldTuple, TableTuple &newTuple) {
    // this approach is far from optimal, but should be technically correct
    processTupleDelete(oldTuple);
    processTupleInsert(newTuple);
}

void MaterializedViewMetadata::processTupleDelete(TableTuple &oldTuple) {
    // don't change the view if this tuple doesn't match the predicate
    if (m_filterPredicate && (m_filterPredicate->eval(&oldTuple, NULL).isFalse()))
        return;

    // this will assert if the tuple isn't there as param expected is true
    findExistingTuple(oldTuple, true);

    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);

    //printf("  Existing tuple: %s.\n", m_existingTuple.debugNoHeader().c_str());
    //fflush(stdout);

    // set up the first column, which is a count
    NValue count = m_existingTuple.getNValue(m_groupByColumnCount).op_decrement();

    //printf("  Count is: %d.\n", (int)(m_existingTuple.getSlimValue(m_groupByColumnCount).getBigInt()));
    //fflush(stdout);

    // check if we should remove the tuple
    if (count.isZero()) {
        m_target->deleteTuple(m_existingTuple, true);
        return;
    }
    // assume from here that we're just updating the existing row

    int colindex = 0;
    // set up the first n columns, based on group-by columns
    for (colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        // note that if the tuple is in the mv's target table,
        // tuple values should be pulled from the existing tuple in
        // that table. This works around a memory ownership issue
        // related to out-of-line strings.
        m_updatedTuple.setNValue(colindex, m_existingTuple.getNValue(colindex));
    }

    m_updatedTuple.setNValue(colindex, count);
    colindex++;

    // set values for the other columns
    for (int i = colindex; i < m_outputColumnCount; i++) {
        NValue oldValue = oldTuple.getNValue(m_outputColumnSrcTableIndexes[i]);
        NValue existingValue = m_existingTuple.getNValue(i);

        if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_SUM) {
            m_updatedTuple.setNValue(i, existingValue.op_subtract(oldValue));
        }
        else if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_COUNT) {
            m_updatedTuple.setNValue(i, existingValue.op_decrement());
        }
        else {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "Error in materialized view table"
                                          " update.");
        }
    }

    // update the row
    // shouldn't need to update indexes as this shouldn't ever change the key
    m_target->updateTuple(m_updatedTuple, m_existingTuple, false);
}

bool MaterializedViewMetadata::findExistingTuple(TableTuple &oldTuple, bool expected) {
    // find the key for this tuple (which is the group by columns)
    for (int i = 0; i < m_groupByColumnCount; i++) {
        m_searchKey.setNValue(i, oldTuple.getNValue(m_groupByColumns[i]));
    }

    // determine if the row exists (create the empty one if it doesn't)
    m_index->moveToKey(&m_searchKey);
    m_existingTuple = m_index->nextValueAtKey();
    if (m_existingTuple.isNullTuple()) {
        if (expected) {
            std::string name = m_target->name();
            throwFatalException("MaterializedViewMetadata for table %s went"
                                " looking for a tuple in the view and"
                                " expected to find it but didn't", name.c_str());
        }
        return false;
    }
    else {
        return true;
    }
}

} // namespace voltdb
