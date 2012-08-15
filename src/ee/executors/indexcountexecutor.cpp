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

#include "indexcountexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"

#include "plannodes/indexcountnode.h"

#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
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

    // Create output table based on output schema from the plan
    TupleSchema* schema = m_node->generateTupleSchema(false);
    int column_count = static_cast<int>(m_node->getOutputSchema().size());
    assert(column_count == 1);

    std::string* column_names = new std::string[column_count];
    column_names[0] = m_node->getOutputSchema()[0]->getColumnName();

    m_node->setOutputTable(TableFactory::getTempTable(m_node->databaseId(),
                                                      m_node->getTargetTable()->name(),
                                                      schema,
                                                      column_names,
                                                      limits));
    delete[] column_names;

    //
    // Make sure that we have search keys and that they're not null
    //
    m_numOfSearchkeys = (int)m_node->getSearchKeyExpressions().size();
    m_searchKeyBeforeSubstituteArrayPtr =
      boost::shared_array<AbstractExpression*>
        (new AbstractExpression*[m_numOfSearchkeys]);
    m_searchKeyBeforeSubstituteArray = m_searchKeyBeforeSubstituteArrayPtr.get();
    m_needsSubstituteSearchKeyPtr =
        boost::shared_array<bool>(new bool[m_numOfSearchkeys]);
    m_needsSubstituteSearchKey = m_needsSubstituteSearchKeyPtr.get();

    for (int ctr = 0; ctr < m_numOfSearchkeys; ctr++)
    {
        if (m_node->getSearchKeyExpressions()[ctr] == NULL)
        {
            VOLT_ERROR("The search key expression at position '%d' is NULL for"
                       " PlanNode '%s'", ctr, m_node->debug().c_str());
            return false;
        }
        m_needsSubstituteSearchKeyPtr[ctr] =
            m_node->getSearchKeyExpressions()[ctr]->hasParameter();
        m_searchKeyBeforeSubstituteArrayPtr[ctr] =
            m_node->getSearchKeyExpressions()[ctr];
    }

    if (m_node->getEndKeyExpressions().size() == 0) {
        m_hasEndKey = false;
    } else {
        m_hasEndKey = true;
        m_numOfEndkeys = (int)m_node->getEndKeyExpressions().size();
        m_endKeyBeforeSubstituteArrayPtr =
                boost::shared_array<AbstractExpression*> (new AbstractExpression*[m_numOfEndkeys]);
        m_endKeyBeforeSubstituteArray = m_endKeyBeforeSubstituteArrayPtr.get();
        m_needsSubstituteEndKeyPtr =
                boost::shared_array<bool>(new bool[m_numOfEndkeys]);
        m_needsSubstituteEndKey = m_needsSubstituteEndKeyPtr.get();
        for (int ctr = 0; ctr < m_numOfEndkeys; ctr++)
        {
            if (m_node->getEndKeyExpressions()[ctr] == NULL) {
                VOLT_ERROR("The end key expression at position '%d' is NULL for"
                        " PlanNode '%s'", ctr, m_node->debug().c_str());
                return false;
            }
            m_needsSubstituteEndKeyPtr[ctr] =
                    m_node->getEndKeyExpressions()[ctr]->hasParameter();
            m_endKeyBeforeSubstituteArrayPtr[ctr] =
                    m_node->getEndKeyExpressions()[ctr];
        }

    }

    //
    // Initialize local variables
    //

    //output table should be temptable
    m_outputTable = static_cast<TempTable*>(m_node->getOutputTable());
    //target table should be persistenttable
    m_targetTable = static_cast<PersistentTable*>(m_node->getTargetTable());
    m_numOfColumns = static_cast<int>(m_outputTable->columnCount());

    assert(m_numOfColumns == 1);
    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    m_index = m_targetTable->index(m_node->getTargetIndexName());
    assert (m_index != NULL);

    // This index should have a true countable flag
    assert(m_index->is_countable_index_);

    m_searchKey = TableTuple(m_index->getKeySchema());
    m_searchKeyBackingStore = new char[m_index->getKeySchema()->tupleLength()];
    m_searchKey.moveNoHeader(m_searchKeyBackingStore);
    if (m_hasEndKey) {
        m_endKey = TableTuple(m_index->getKeySchema());
        m_endKeyBackingStore = new char[m_index->getKeySchema()->tupleLength()];
        m_endKey.moveNoHeader(m_endKeyBackingStore);
    }

    m_tuple = TableTuple(m_targetTable->schema());

    if (m_node->getPredicate() != NULL)
    {
        m_needsSubstitutePostExpression =
            m_node->getPredicate()->hasParameter();
    }

    //
    // Miscellanous Information
    //
    m_lookupType = m_node->getLookupType();
    if (m_hasEndKey) {
        m_endType = m_node->getEndType();
    }

    // Need to move GTE to find (x,_) when doing a partial covering search.
    // the planner sometimes lies in this case: index_lookup_type_eq is incorrect.
    // Index_lookup_type_gte is necessary. Make the change here.
    if (m_lookupType == INDEX_LOOKUP_TYPE_EQ &&
        m_searchKey.getSchema()->columnCount() > m_numOfSearchkeys)
    {
        VOLT_TRACE("Setting lookup type to GTE for partial covering key.");
        m_lookupType = INDEX_LOOKUP_TYPE_GTE;
    }

    return true;
}

bool IndexCountExecutor::p_execute(const NValueArray &params)
{
    assert(m_node);
    assert(m_node == dynamic_cast<IndexCountPlanNode*>(m_abstractNode));
    assert(m_outputTable);
    assert(m_outputTable == static_cast<TempTable*>(m_node->getOutputTable()));
    assert(m_targetTable);
    assert(m_targetTable == m_node->getTargetTable());
    VOLT_DEBUG("IndexCount: %s.%s\n", m_targetTable->name().c_str(),
               m_index->getName().c_str());

    int activeNumOfSearchKeys = m_numOfSearchkeys;
    IndexLookupType localLookupType = m_lookupType;
    bool searchKeyUnderflow = false, endKeyOverflow = false;

    //
    // SEARCH KEY
    //
    m_searchKey.setAllNulls();
    VOLT_TRACE("Initial (all null) search key: '%s'", m_searchKey.debugNoHeader().c_str());
    for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
        if (m_needsSubstituteSearchKey[ctr]) {
            m_searchKeyBeforeSubstituteArray[ctr]->substitute(params);
        }
        NValue candidateValue = m_searchKeyBeforeSubstituteArray[ctr]->eval(&m_dummy, NULL);
        try {
            m_searchKey.setNValue(ctr, candidateValue);
        }
        catch (SQLException e) {
            // This next bit of logic handles underflow and overflow while
            // setting up the search keys.
            // e.g. TINYINT > 200 or INT <= 6000000000
            // rethow if not an overflow - currently, it's expected to always be an overflow
            if (e.getSqlState() != SQLException::data_exception_numeric_value_out_of_range) {
                throw e;
            }

            // handle the case where this is a comparison, rather than equality match
            // comparison is the only place where the executor might return matching tuples
            // e.g. TINYINT < 1000 should return all values
            TableTuple& tmptup = m_outputTable->tempTuple();
            tmptup.setNValue(0, ValueFactory::getBigIntValue( 0 ));

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
    assert(activeNumOfSearchKeys > 0);
    VOLT_TRACE("Search key after substitutions: '%s'", m_searchKey.debugNoHeader().c_str());

    int activeNumOfEndKeys = -1;
    if (m_hasEndKey) {
        activeNumOfEndKeys = m_numOfEndkeys;
        //
        // END KEY
        //
        m_endKey.setAllNulls();
        VOLT_TRACE("Initial (all null) end key: '%s'", m_endKey.debugNoHeader().c_str());
        for (int ctr = 0; ctr < activeNumOfEndKeys; ctr++) {
            if (m_needsSubstituteEndKey[ctr]) {
                m_endKeyBeforeSubstituteArray[ctr]->substitute(params);
            }
            NValue endKeyValue = m_endKeyBeforeSubstituteArray[ctr]->eval(&m_dummy, NULL);
            try {
                m_endKey.setNValue(ctr, endKeyValue);
            }
            catch (SQLException e) {
                // This next bit of logic handles underflow and overflow while
                // setting up the search keys.
                // e.g. TINYINT > 200 or INT <= 6000000000

                // rethow if not an overflow - currently, it's expected to always be an overflow
                if (e.getSqlState() != SQLException::data_exception_numeric_value_out_of_range) {
                    throw e;
                }

                // handle the case where this is a comparison, rather than equality match
                // comparison is the only place where the executor might return matching tuples
                // e.g. TINYINT < 1000 should return all values
                TableTuple& tmptup = m_outputTable->tempTuple();
                tmptup.setNValue(0, ValueFactory::getBigIntValue( 0 ));

                if (ctr == (activeNumOfEndKeys - 1)) {
                    assert (m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE);
                    if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        m_outputTable->insertTuple(tmptup);
                        return true;
                    } else if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        endKeyOverflow = true;
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
        //assert((activeNumOfEndKeys == 0) || (m_endKey.getSchema()->columnCount() > 0));
        VOLT_TRACE("End key after substitutions: '%s'", m_endKey.debugNoHeader().c_str());
    }

    //
    // POST EXPRESSION
    //
    assert (m_node->getPredicate() == NULL);

    assert (m_index);
    assert (m_index == m_targetTable->index(m_node->getTargetIndexName()));
    assert (m_index->is_countable_index_);

    // An index count has two parts: unique and multi

    int64_t rkStart = 0, rkEnd = 0, rkRes = 0;

    TableTuple& tmptup = m_outputTable->tempTuple();
    int leftIncluded = 0, rightIncluded = 0;

    if (m_index->isUniqueIndex()) {
        // Deal with unique map
        VOLT_DEBUG("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                localLookupType, activeNumOfSearchKeys, m_searchKey.debugNoHeader().c_str());
        if (searchKeyUnderflow == false) {
            if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
                rkStart = m_index->getCounterLET(&m_searchKey, false);
            } else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
                rkStart = m_index->getCounterLET(&m_searchKey, false);
                if (m_index->hasKey(&m_searchKey))
                    leftIncluded = 1;
                if (m_searchKey.getSchema()->columnCount() > activeNumOfSearchKeys) {
                    // search key is not complete:
                    // like: SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS < ?
                    // like: SELECT count(*) from T2 WHERE POINTS < ?
                    // but it actually finds the previous rank. (If m_searchKey is null, find 0 rank)
                    // Add 1 back.
                    rkStart++;
                    leftIncluded = 1;
                }
            } else {
                return false;
            }
        }

        if (m_hasEndKey) {
            if (endKeyOverflow == false) {
                IndexLookupType localEndType = m_endType;
                if (localEndType == INDEX_LOOKUP_TYPE_LT) {
                    rkEnd = m_index->getCounterGET(&m_endKey, false);
                } else if (localEndType == INDEX_LOOKUP_TYPE_LTE) {
                    rkEnd = m_index->getCounterGET(&m_endKey, false);
                    if (m_index->hasKey(&m_endKey))
                        rightIncluded = 1;
                } else {
                    return false;
                }
            } else {
                rkEnd = m_index->getSize();
                rightIncluded = 1;
            }
        } else {
            rkEnd = m_index->getSize();
            rightIncluded = 1;
        }
    } else {
        // Deal with multi-map
        VOLT_DEBUG("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                localLookupType, activeNumOfSearchKeys, m_searchKey.debugNoHeader().c_str());
        if (searchKeyUnderflow == false) {
            if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
                rkStart = m_index->getCounterLET(&m_searchKey, true);
            } else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
                if (m_index->hasKey(&m_searchKey)) {
                    leftIncluded = 1;
                    rkStart = m_index->getCounterLET(&m_searchKey, false);
                } else {
                    rkStart = m_index->getCounterLET(&m_searchKey, true);
                }
                if (m_searchKey.getSchema()->columnCount() > activeNumOfSearchKeys) {
                    // search key is not complete:
                    // like: SELECT count(*) from T2 WHERE USERNAME ='XIN' AND POINTS < ?
                    // like: SELECT count(*) from T2 WHERE POINTS < ?
                    // but it actually finds the previous rank. (If m_searchKey is null, find 0 rank)
                    // Add 1 back.
                    rkStart++;
                    leftIncluded = 1;
                }
            } else {
                return false;
            }
        }
        if (m_hasEndKey) {
            if (endKeyOverflow == false) {
                IndexLookupType localEndType = m_endType;
                if (localEndType == INDEX_LOOKUP_TYPE_LT) {
                    rkEnd = m_index->getCounterGET(&m_endKey, false);
                } else if (localEndType == INDEX_LOOKUP_TYPE_LTE) {
                    if (m_index->hasKey(&m_endKey)) {
                        rkEnd = m_index->getCounterGET(&m_endKey, true);
                        rightIncluded = 1;
                    } else
                        rkEnd = m_index->getCounterGET(&m_endKey, false);
                } else {
                    return false;
                }
            } else {
                rkEnd = m_index->getSize();
                rightIncluded = 1;
            }
        } else {
            rkEnd = m_index->getSize();
            rightIncluded = 1;
        }
    }
    rkRes = rkEnd - rkStart - 1 + leftIncluded + rightIncluded;
    VOLT_DEBUG("ANSWER %ld = %ld - %ld - 1 + %d + %d\n", (long)rkRes, (long)rkEnd, (long)rkStart, leftIncluded, rightIncluded);
    tmptup.setNValue(0, ValueFactory::getBigIntValue( rkRes ));
    m_outputTable->insertTuple(tmptup);

    VOLT_DEBUG ("Index Count :\n %s", m_outputTable->debug().c_str());
    return true;
}

IndexCountExecutor::~IndexCountExecutor() {
    delete [] m_searchKeyBackingStore;
    if (m_hasEndKey)
        delete [] m_endKeyBackingStore;
}
