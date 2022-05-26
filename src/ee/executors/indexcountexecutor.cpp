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

#include "indexcountexecutor.h"

#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"
#include "plannodes/indexcountnode.h"
#include "storage/tableiterator.h"
#include "storage/persistenttable.h"

using namespace voltdb;

static long countNulls(TableIndex * tableIndex, AbstractExpression * countNULLExpr,
        IndexCursor& indexCursor) {
    if (countNULLExpr == NULL) {
        return 0;
    }
    long numNULLs = 0;
    TableTuple tuple;
    while (! (tuple = tableIndex->nextValue(indexCursor)).isNullTuple()) {
         if (! countNULLExpr->eval(&tuple, NULL).isTrue()) {
             break;
         }
        numNULLs++;
    }
    return numNULLs;
}


bool IndexCountExecutor::p_init(
        AbstractPlanNode *abstractNode, const ExecutorVector& executorVector) {
    VOLT_DEBUG("init IndexCount Executor");

    m_node = dynamic_cast<IndexCountPlanNode*>(abstractNode);
    vassert(m_node);
    vassert(m_node->getTargetTable());
    vassert(m_node->getPredicate() == NULL);

    // Create output table based on output schema from the plan
    setTempOutputTable(executorVector);

    //
    // Make sure that we have search keys and that they're not null
    //
    m_numOfSearchkeys = (int)m_node->getSearchKeyExpressions().size();
    if (m_numOfSearchkeys != 0) {
        m_searchKeyArrayPtr =
            boost::shared_array<AbstractExpression*>
            (new AbstractExpression*[m_numOfSearchkeys]);
        m_searchKeyArray = m_searchKeyArrayPtr.get();

        for (int ctr = 0; ctr < m_numOfSearchkeys; ctr++) {
            if (m_node->getSearchKeyExpressions()[ctr] == NULL) {
                VOLT_ERROR("The search key expression at position '%d' is NULL for"
                    " PlanNode '%s'", ctr, m_node->debug().c_str());
                return false;
            }
            m_searchKeyArrayPtr[ctr] = m_node->getSearchKeyExpressions()[ctr];
        }
    }

    m_numOfEndkeys = (int)m_node->getEndKeyExpressions().size();
    if (m_numOfEndkeys != 0) {
        m_endKeyArrayPtr =
            boost::shared_array<AbstractExpression*> (new AbstractExpression*[m_numOfEndkeys]);
        m_endKeyArray = m_endKeyArrayPtr.get();
        for (int ctr = 0; ctr < m_numOfEndkeys; ctr++)
        {
            if (m_node->getEndKeyExpressions()[ctr] == NULL) {
                VOLT_ERROR("The end key expression at position '%d' is NULL for"
                    " PlanNode '%s'", ctr, m_node->debug().c_str());
                return false;
            }
            m_endKeyArrayPtr[ctr] = m_node->getEndKeyExpressions()[ctr];
        }
    }
    //output table should be temptable
    m_outputTable = static_cast<AbstractTempTable*>(m_node->getOutputTable());
    m_numOfColumns = static_cast<int>(m_outputTable->columnCount());
    vassert(m_numOfColumns == 1);

    // Miscellanous Information
    m_lookupType = IndexLookupType::Invalid;
    if (m_numOfSearchkeys != 0) {
        m_lookupType = m_node->getLookupType();
    }

    if (m_numOfEndkeys != 0) {
        m_endType = m_node->getEndType();
    }

    // The target table should be a persistent table
    // Grab the Index from the table. Throw an error if the index is missing.
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(m_node->getTargetTable());
    vassert(targetTable);

    TableIndex *tableIndex = targetTable->index(m_node->getTargetIndexName());
    vassert(tableIndex != NULL);

    // This index should have a true countable flag
    vassert(tableIndex->isCountableIndex());

    if (m_numOfSearchkeys != 0) {
        m_searchKeyBackingStore = new char[tableIndex->getKeySchema()->tupleLength()];
    }

    if (m_numOfEndkeys != 0) {
        m_endKeyBackingStore = new char[tableIndex->getKeySchema()->tupleLength()];
    }

    VOLT_DEBUG("IndexCount: %s.%s\n", targetTable->name().c_str(),
            tableIndex->getName().c_str());

    return true;
}

bool IndexCountExecutor::p_execute(const NValueArray &params) {
    // update local target table with its most recent reference
    vassert(dynamic_cast<PersistentTable*>(m_node->getTargetTable()));
    PersistentTable* targetTable = static_cast<PersistentTable*>(m_node->getTargetTable());
    TableIndex * tableIndex = targetTable->index(m_node->getTargetIndexName());
    IndexCursor indexCursor(tableIndex->getTupleSchema());

    TableTuple searchKey, endKey;
    if (m_numOfSearchkeys != 0) {
        searchKey = TableTuple(tableIndex->getKeySchema());
        searchKey.moveNoHeader(m_searchKeyBackingStore);
    }
    if (m_numOfEndkeys != 0) {
        endKey = TableTuple(tableIndex->getKeySchema());
        endKey.moveNoHeader(m_endKeyBackingStore);
    }

    // Need to move GTE to find (x,_) when doing a partial covering search.
    // The planner sometimes used to lie in this case: index_lookup_type_eq is incorrect.
    // Index_lookup_type_gte is necessary.
    vassert(m_lookupType != IndexLookupType::Equal ||
            searchKey.getSchema()->columnCount() == m_numOfSearchkeys ||
            searchKey.getSchema()->columnCount() == m_numOfEndkeys);

    int activeNumOfSearchKeys = m_numOfSearchkeys;
    IndexLookupType localLookupType = m_lookupType;
    bool searchKeyUnderflow = false, endKeyOverflow = false;
    // Overflow cases that can return early without accessing the index need this
    // default 0 count as their result.
    TableTuple& tmptup = m_outputTable->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue( 0 ));

    bool earlyReturnForSearchKeyOutOfRange = false;
    //
    // SEARCH KEY
    //
    if (m_numOfSearchkeys != 0) {
        searchKey.setAllNulls();
        VOLT_DEBUG("<Index Count>Initial (all null) search key: '%s'", searchKey.debugNoHeader().c_str());
        for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
            NValue candidateValue = m_searchKeyArray[ctr]->eval(NULL, NULL);
            if (candidateValue.isNull() && m_node->getCompareNotDistinctFlags()[ctr] == false) {
                // when any part of the search key is NULL, the result is false when it compares to anything.
                // do early return optimization, our index comparator may not handle null comparison correctly.
                earlyReturnForSearchKeyOutOfRange = true;
                break;
            }
            try {
                searchKey.setNValue(ctr, candidateValue);
            } catch (const SQLException &e) {
                // This next bit of logic handles underflow, overflow and search key length
                // exceeding variable length column size (variable lenght mismatch) when
                // setting up the search keys.
                // e.g. TINYINT > 200 or INT <= 6000000000
                // VarChar(3 bytes) < "abcd" or VarChar(3) > "abbd"
                //
                // Shouldn't this all be the same as the code in indexscanexecutor?
                // Here the localLookupType can only be NE, EQ, GT or GTE, and never LT
                // or LTE.  But that seems like something a template could puzzle out.
                //
                // re-throw if not an overflow or underflow or variable length mismatch
                // currently, it's expected to always be an overflow or underflow
                if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW | SQLException::TYPE_VAR_LENGTH_MISMATCH)) == 0) {
                    throw e;
                }

                // handle the case where this is a comparison, rather than equality match
                // comparison is the only place where the executor might return matching tuples
                // e.g. TINYINT < 1000 should return all values
                if (localLookupType != IndexLookupType::Equal && ctr == (activeNumOfSearchKeys - 1)) {
                    vassert(localLookupType == IndexLookupType::Greater || localLookupType == IndexLookupType::GreaterEqual);

                    // See throwCastSQLValueOutOfRangeException to see that
                    // these three cases, TYPE_OVERFLOW, TYPE_UNDERFLOW and
                    // TYPE_VAR_LENGTH_MISMATCH are orthogonal.
                    if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        earlyReturnForSearchKeyOutOfRange = true;
                        break;
                    } else if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        searchKeyUnderflow = true;
                        break;
                    } else if (e.getInternalFlags() & SQLException::TYPE_VAR_LENGTH_MISMATCH) {
                        // shrink the search key and add the updated key to search key table tuple
                        searchKey.shrinkAndSetNValue(ctr, candidateValue);
                        // search will be performed on shrinked key, so update lookup operation
                        // to account for it.  We think localLookupType can only be
                        // GT and GTE here (cf. the assert above).
                        switch (localLookupType) {
                            case IndexLookupType::Greater:
                            case IndexLookupType::GreaterEqual:
                                localLookupType = IndexLookupType::Greater;
                                break;
                            default:
                                vassert(!"IndexCountExecutor::p_execute - can't index on not equals");
                                return false;
                        }
                    }
                } else { // if a EQ comparision is out of range, then return no tuples
                    earlyReturnForSearchKeyOutOfRange = true;
                    break;
                }
                break;
            }
        }
        VOLT_TRACE("Search key after substitutions: '%s'", searchKey.debugNoHeader().c_str());

        if (earlyReturnForSearchKeyOutOfRange) {
            m_outputTable->insertTuple(tmptup);
            return true;
        }
    }

    if (m_numOfEndkeys != 0) {
        //
        // END KEY
        //
        endKey.setAllNulls();
        VOLT_DEBUG("Initial (all null) end key: '%s'", endKey.debugNoHeader().c_str());
        for (int ctr = 0; ctr < m_numOfEndkeys; ctr++) {
            NValue endKeyValue = m_endKeyArray[ctr]->eval(NULL, NULL);
            if (endKeyValue.isNull() && m_node->getCompareNotDistinctFlags()[ctr] == false) {
                // when any part of the search key is NULL, the result is false when it compares to anything.
                // do early return optimization, our index comparator may not handle null comparison correctly.
                earlyReturnForSearchKeyOutOfRange = true;
                break;
            }
            try {
                endKey.setNValue(ctr, endKeyValue);
            } catch (const SQLException &e) {
                // This next bit of logic handles underflow and overflow while
                // setting up the search keys.
                // e.g. TINYINT > 200 or INT <= 6000000000

                // re-throw if not an overflow or underflow or TYPE_VAR_LENGTH_MISMATCH.
                if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW |
                                SQLException::TYPE_VAR_LENGTH_MISMATCH)) == 0) {
                    throw e;
                } else if (ctr == (m_numOfEndkeys - 1)) {
                    vassert(m_endType == IndexLookupType::Less || m_endType == IndexLookupType::LessEqual);
                    if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        earlyReturnForSearchKeyOutOfRange = true;
                        break;
                    } else if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        endKeyOverflow = true;
                        //
                        // We promise never to generate an end key unless the
                        // following will work.  That is to say, unless the
                        // end key type is some kind of integer.  Will DECIMAL
                        // even work here?  FLOAT won't work at all.
                        //
                        const ValueType type = endKey.getSchema()->columnType(ctr);
                        NValue tmpEndKeyValue = ValueFactory::getBigIntValue(getMaxTypeValue(type));
                        endKey.setNValue(ctr, tmpEndKeyValue);

                        VOLT_DEBUG("<Index count> end key out of range, MAX value: %ld...\n", (long)getMaxTypeValue(type));
                        break;
                    } else if (e.getInternalFlags() & SQLException::TYPE_VAR_LENGTH_MISMATCH) {
                        // shrink the end key and add the updated key to end key table tuple
                        endKey.shrinkAndSetNValue(ctr, endKeyValue);
                        // search will be performed on shrinked key, so update lookup operation
                        // to account for it
                        switch (m_endType) {
                            case IndexLookupType::Less:
                            case IndexLookupType::LessEqual:
                                m_endType = IndexLookupType::LessEqual;
                                break;
                            default:
                                vassert(!"IndexCountExecutor::p_execute - invalid end type.");
                                return false;
                        }
                    }
                } else { // if a EQ comparision is out of range, then return no tuples
                    earlyReturnForSearchKeyOutOfRange = true;
                    break;
                }
                break;
            }
        }
        VOLT_TRACE("End key after substitutions: '%s'", endKey.debugNoHeader().c_str());

        if (earlyReturnForSearchKeyOutOfRange) {
            m_outputTable->insertTuple(tmptup);
            return true;
        }
    }

    //
    // POST EXPRESSION
    //
    vassert(m_node->getPredicate() == NULL);

    //
    // COUNT NULL EXPRESSION
    //
    AbstractExpression* countNULLExpr = m_node->getSkipNullPredicate();
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (countNULLExpr != NULL) {
        VOLT_DEBUG("COUNT NULL Expression:\n%s", countNULLExpr->debug(true).c_str());
    }

    bool reverseScanNullEdgeCase = false;
    bool reverseScanMovedIndexToScan = false;
    if (m_numOfSearchkeys < m_numOfEndkeys &&
            (m_endType == IndexLookupType::Less || m_endType == IndexLookupType::LessEqual)) {
        reverseScanNullEdgeCase = true;
        VOLT_DEBUG("Index count: reverse scan edge null case." );
    }


    // An index count has two cases: unique and non-unique
    int64_t rkStart = 0, rkEnd = 0, rkRes = 0;
    int leftIncluded = 0, rightIncluded = 0;

    if (m_numOfSearchkeys != 0) {
        // Deal with multi-map
        VOLT_DEBUG("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                   static_cast<int>(localLookupType), activeNumOfSearchKeys, searchKey.debugNoHeader().c_str());
        if (searchKeyUnderflow == false) {
            if (localLookupType == IndexLookupType::Greater) {
                rkStart = tableIndex->getCounterLET(&searchKey, true, indexCursor);
            } else if (tableIndex->hasKey(&searchKey)) { // handle start inclusive cases.
                leftIncluded = 1;
                rkStart = tableIndex->getCounterLET(&searchKey, false, indexCursor);

                if (reverseScanNullEdgeCase) {
                    tableIndex->moveToKeyOrGreater(&searchKey, indexCursor);
                    reverseScanMovedIndexToScan = true;
                }
            } else {
                rkStart = tableIndex->getCounterLET(&searchKey, true, indexCursor);
            }
        } else {
            // Do not count null row or columns
            tableIndex->moveToKeyOrGreater(&searchKey, indexCursor);
            vassert(countNULLExpr);
            long numNULLs = countNulls(tableIndex, countNULLExpr, indexCursor);
            rkStart += numNULLs;
            VOLT_DEBUG("Index count[underflow case]: "
                    "find out %ld null rows or columns are not counted in.", numNULLs);

        }
    }
    if (reverseScanNullEdgeCase) {
        // reverse scan case
        if (!reverseScanMovedIndexToScan && localLookupType != IndexLookupType::Greater) {
            tableIndex->moveToEnd(true, indexCursor);
        }
        vassert(countNULLExpr);
        long numNULLs = countNulls(tableIndex, countNULLExpr, indexCursor);
        rkStart += numNULLs;
        VOLT_DEBUG("Index count[reverse case]: "
                "find out %ld null rows or columns are not counted in.", numNULLs);
    }

    if (m_numOfEndkeys != 0) {
        if (endKeyOverflow) {
            rkEnd = tableIndex->getCounterGET(&endKey, true, indexCursor);
        } else {
            IndexLookupType localEndType = m_endType;
            if (localEndType == IndexLookupType::Less) {
                rkEnd = tableIndex->getCounterGET(&endKey, false, indexCursor);
            } else if (tableIndex->hasKey(&endKey)) {
                rightIncluded = 1;
                rkEnd = tableIndex->getCounterGET(&endKey, true, indexCursor);
            } else {
                rkEnd = tableIndex->getCounterGET(&endKey, false, indexCursor);
            }
        }
    } else {
        rkEnd = tableIndex->getSize();
        rightIncluded = 1;
    }
    rkRes = rkEnd - rkStart - 1 + leftIncluded + rightIncluded;
    VOLT_DEBUG("Index Count ANSWER %ld = %ld - %ld - 1 + %d + %d\n",
            (long)rkRes, (long)rkEnd, (long)rkStart, leftIncluded, rightIncluded);
    tmptup.setNValue(0, ValueFactory::getBigIntValue( rkRes ));
    m_outputTable->insertTuple(tmptup);

    VOLT_DEBUG ("Index Count :\n %s", m_outputTable->debug().c_str());
    return true;
}

IndexCountExecutor::~IndexCountExecutor() {
    if (m_numOfSearchkeys != 0) {
        delete [] m_searchKeyBackingStore;
    }
    if (m_numOfEndkeys != 0) {
        delete [] m_endKeyBackingStore;
    }
}

