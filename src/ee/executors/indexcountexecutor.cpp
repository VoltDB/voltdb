/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"
#include "plannodes/indexcountnode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace voltdb;

bool IndexCountExecutor::p_init(AbstractPlanNode *abstractNode,
                                TempTableLimits* limits)
{
    VOLT_DEBUG("init IndexCount Executor");

    m_node = dynamic_cast<IndexCountPlanNode*>(abstractNode);
    assert(m_node);
    assert(m_node->getTargetTable());
    assert(m_node->getPredicate() == NULL);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits);

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
    m_outputTable = static_cast<TempTable*>(m_node->getOutputTable());
    m_numOfColumns = static_cast<int>(m_outputTable->columnCount());
    assert(m_numOfColumns == 1);

    // Miscellanous Information
    m_lookupType = INDEX_LOOKUP_TYPE_INVALID;
    if (m_numOfSearchkeys != 0) {
        m_lookupType = m_node->getLookupType();
    }

    if (m_numOfEndkeys != 0) {
        m_endType = m_node->getEndType();
    }

    //target table should be persistenttable
    m_targetTable = static_cast<PersistentTable*>(m_node->getTargetTable());
    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    m_index = m_targetTable->index(m_node->getTargetIndexName());
    assert (m_index != NULL);

    // This index should have a true countable flag
    assert(m_index->isCountableIndex());

    if (m_numOfSearchkeys != 0) {
        m_searchKey = TableTuple(m_index->getKeySchema());
        m_searchKeyBackingStore = new char[m_index->getKeySchema()->tupleLength()];
        m_searchKey.moveNoHeader(m_searchKeyBackingStore);
    }

    if (m_numOfEndkeys != 0) {
        m_endKey = TableTuple(m_index->getKeySchema());
        m_endKeyBackingStore = new char[m_index->getKeySchema()->tupleLength()];
        m_endKey.moveNoHeader(m_endKeyBackingStore);
    }

    // Need to move GTE to find (x,_) when doing a partial covering search.
    // The planner sometimes used to lie in this case: index_lookup_type_eq is incorrect.
    // Index_lookup_type_gte is necessary.
    assert(m_lookupType != INDEX_LOOKUP_TYPE_EQ ||
            m_searchKey.getSchema()->columnCount() == m_numOfSearchkeys ||
            m_searchKey.getSchema()->columnCount() == m_numOfEndkeys);


    VOLT_DEBUG("IndexCount: %s.%s\n", m_targetTable->name().c_str(),
            m_index->getName().c_str());

    return true;
}

void IndexCountExecutor::updateTargetTableAndIndex() {
    //target table should be persistenttable
    m_targetTable = static_cast<PersistentTable*>(m_node->getTargetTable());
    assert(m_targetTable);
    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    m_index = m_targetTable->index(m_node->getTargetIndexName());
    assert (m_index != NULL);
}

bool IndexCountExecutor::p_execute(const NValueArray &params)
{
    assert(m_node);
    assert(m_node == dynamic_cast<IndexCountPlanNode*>(m_abstractNode));
    assert(m_outputTable);
    assert(m_outputTable == static_cast<TempTable*>(m_node->getOutputTable()));

    // update local target table with its most recent reference
    updateTargetTableAndIndex();

    int activeNumOfSearchKeys = m_numOfSearchkeys;
    IndexLookupType localLookupType = m_lookupType;
    bool searchKeyUnderflow = false, endKeyOverflow = false;
    // Overflow cases that can return early without accessing the index need this
    // default 0 count as their result.
    TableTuple& tmptup = m_outputTable->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue( 0 ));

    //
    // SEARCH KEY
    //
    if (m_numOfSearchkeys != 0) {
        m_searchKey.setAllNulls();
        VOLT_DEBUG("<Index Count>Initial (all null) search key: '%s'", m_searchKey.debugNoHeader().c_str());
        for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
            m_searchKeyArray[ctr]->substitute(params);
            NValue candidateValue = m_searchKeyArray[ctr]->eval(NULL, NULL);
            try {
                m_searchKey.setNValue(ctr, candidateValue);
            }
            catch (const SQLException &e) {
                // This next bit of logic handles underflow and overflow while
                // setting up the search keys.
                // e.g. TINYINT > 200 or INT <= 6000000000

                // re-throw if not an overflow or underflow
                // currently, it's expected to always be an overflow or underflow
                if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW)) == 0) {
                    throw e;
                }

                // handle the case where this is a comparison, rather than equality match
                // comparison is the only place where the executor might return matching tuples
                // e.g. TINYINT < 1000 should return all values

                if ((localLookupType != INDEX_LOOKUP_TYPE_EQ) &&
                    (ctr == (activeNumOfSearchKeys - 1))) {
                    assert (localLookupType == INDEX_LOOKUP_TYPE_GT || localLookupType == INDEX_LOOKUP_TYPE_GTE);

                    if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        m_outputTable->insertTuple(tmptup);
                        return true;
                    } else if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        searchKeyUnderflow = true;
                        break;
                    } else {
                        throw e;
                    }
                }
                // if a EQ comparision is out of range, then return no tuples
                else {
                    m_outputTable->insertTuple(tmptup);
                    return true;
                }
                break;
            }
        }
        VOLT_TRACE("Search key after substitutions: '%s'", m_searchKey.debugNoHeader().c_str());
    }

    if (m_numOfEndkeys != 0) {
        //
        // END KEY
        //
        m_endKey.setAllNulls();
        VOLT_DEBUG("Initial (all null) end key: '%s'", m_endKey.debugNoHeader().c_str());
        for (int ctr = 0; ctr < m_numOfEndkeys; ctr++) {
            m_endKeyArray[ctr]->substitute(params);
            NValue endKeyValue = m_endKeyArray[ctr]->eval(NULL, NULL);
            try {
                m_endKey.setNValue(ctr, endKeyValue);
            }
            catch (const SQLException &e) {
                // This next bit of logic handles underflow and overflow while
                // setting up the search keys.
                // e.g. TINYINT > 200 or INT <= 6000000000

                // re-throw if not an overflow or underflow
                // currently, it's expected to always be an overflow or underflow
                if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW)) == 0) {
                    throw e;
                }

                if (ctr == (m_numOfEndkeys - 1)) {
                    assert (m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE);
                    if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        m_outputTable->insertTuple(tmptup);
                        return true;
                    } else if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        endKeyOverflow = true;
                        const ValueType type = m_endKey.getSchema()->columnType(ctr);
                        NValue tmpEndKeyValue = ValueFactory::getBigIntValue(getMaxTypeValue(type));
                        m_endKey.setNValue(ctr, tmpEndKeyValue);

                        VOLT_DEBUG("<Index count> end key out of range, MAX value: %ld...\n", (long)getMaxTypeValue(type));
                        break;
                    } else {
                        throw e;
                    }
                }
                // if a EQ comparision is out of range, then return no tuples
                else {
                    m_outputTable->insertTuple(tmptup);
                    return true;
                }
                break;
            }
        }
        VOLT_TRACE("End key after substitutions: '%s'", m_endKey.debugNoHeader().c_str());
    }

    //
    // POST EXPRESSION
    //
    assert (m_node->getPredicate() == NULL);

    assert (m_index);
    assert (m_index == m_targetTable->index(m_node->getTargetIndexName()));
    assert (m_index->isCountableIndex());

    //
    // COUNT NULL EXPRESSION
    //
    AbstractExpression* countNULLExpr = m_node->getSkipNullPredicate();
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (countNULLExpr != NULL) {
        countNULLExpr->substitute(params);
        VOLT_DEBUG("COUNT NULL Expression:\n%s", countNULLExpr->debug(true).c_str());
    }

    bool reverseScanNullEdgeCase = false;
    bool reverseScanMovedIndexToScan = false;
    if (m_numOfSearchkeys < m_numOfEndkeys && (m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE)) {
        reverseScanNullEdgeCase = true;
        VOLT_DEBUG("Index count: reverse scan edge null case." );
    }


    // An index count has two cases: unique and non-unique
    int64_t rkStart = 0, rkEnd = 0, rkRes = 0;
    int leftIncluded = 0, rightIncluded = 0;

    if (m_numOfSearchkeys != 0) {
        // Deal with multi-map
        VOLT_DEBUG("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                   localLookupType, activeNumOfSearchKeys, m_searchKey.debugNoHeader().c_str());
        if (searchKeyUnderflow == false) {
            if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
                rkStart = m_index->getCounterLET(&m_searchKey, true);
            } else {
                // handle start inclusive cases.
                if (m_index->hasKey(&m_searchKey)) {
                    leftIncluded = 1;
                    rkStart = m_index->getCounterLET(&m_searchKey, false);

                    if (reverseScanNullEdgeCase) {
                        m_index->moveToKeyOrGreater(&m_searchKey);
                        reverseScanMovedIndexToScan = true;
                    }
                } else {
                    rkStart = m_index->getCounterLET(&m_searchKey, true);
                }
            }
        } else {
            // Do not count null row or columns
            m_index->moveToKeyOrGreater(&m_searchKey);
            assert(countNULLExpr);
            long numNULLs = countNulls(countNULLExpr);
            rkStart += numNULLs;
            VOLT_DEBUG("Index count[underflow case]: find out %ld null rows or columns are not counted in.", numNULLs);

        }
    }
    if (reverseScanNullEdgeCase) {
        // reverse scan case
        if (!reverseScanMovedIndexToScan && localLookupType != INDEX_LOOKUP_TYPE_GT) {
            m_index->moveToEnd(true);
        }
        assert(countNULLExpr);
        long numNULLs = countNulls(countNULLExpr);
        rkStart += numNULLs;
        VOLT_DEBUG("Index count[reverse case]: find out %ld null rows or columns are not counted in.", numNULLs);
    }

    if (m_numOfEndkeys != 0) {
        if (endKeyOverflow) {
            rkEnd = m_index->getCounterGET(&m_endKey, true);
        } else {
            IndexLookupType localEndType = m_endType;
            if (localEndType == INDEX_LOOKUP_TYPE_LT) {
                rkEnd = m_index->getCounterGET(&m_endKey, false);
            } else {
                if (m_index->hasKey(&m_endKey)) {
                    rightIncluded = 1;
                    rkEnd = m_index->getCounterGET(&m_endKey, true);
                } else {
                    rkEnd = m_index->getCounterGET(&m_endKey, false);
                }
            }
        }
    } else {
        rkEnd = m_index->getSize();
        rightIncluded = 1;
    }
    rkRes = rkEnd - rkStart - 1 + leftIncluded + rightIncluded;
    VOLT_DEBUG("Index Count ANSWER %ld = %ld - %ld - 1 + %d + %d\n", (long)rkRes, (long)rkEnd, (long)rkStart, leftIncluded, rightIncluded);
    tmptup.setNValue(0, ValueFactory::getBigIntValue( rkRes ));
    m_outputTable->insertTuple(tmptup);

    VOLT_DEBUG ("Index Count :\n %s", m_outputTable->debug().c_str());
    return true;
}


long IndexCountExecutor::countNulls(AbstractExpression * countNULLExpr) {
    if (countNULLExpr == NULL) {
        return 0;
    }
    long numNULLs = 0;
    TableTuple tuple;
    while ( ! (tuple = m_index->nextValue()).isNullTuple()) {
         if ( ! countNULLExpr->eval(&tuple, NULL).isTrue()) {
             break;
         }
        numNULLs++;
    }
    return numNULLs;
}


IndexCountExecutor::~IndexCountExecutor() {
    if (m_numOfSearchkeys != 0) {
        delete [] m_searchKeyBackingStore;
    }
    if (m_numOfEndkeys != 0) {
        delete [] m_endKeyBackingStore;
    }
}
