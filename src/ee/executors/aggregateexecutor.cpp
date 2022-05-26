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

#include <tuple>
#include "common/SerializableEEException.h"
#include "executors/aggregateexecutor.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/limitnode.h"
#include "storage/temptable.h"

#include "hyperloglog/hyperloglog.hpp" // for APPROX_COUNT_DISTINCT

namespace voltdb {
/**
 * Mix-in class to tweak some Aggs' behavior when the DISTINCT flag was specified,
 * It tracks and de-dupes repeated input values.
 * It is specified as a parameter class that determines the type of the ifDistinct data member.
 */
class Distinct : public std::unordered_set<NValue> {
    Pool* m_memoryPool;
public:
    explicit Distinct(Pool* memoryPool) : m_memoryPool(memoryPool) {}
    bool excludeValue(const NValue& val) {
        // find this value in the set.  If it doesn't exist, add
        // it, otherwise indicate it shouldn't be included in the
        // aggregate
        if (find(val) == end()) {
            if (val.getVolatile()) {
                // We only come here in the case of inlined VARCHAR or
                // VARBINARY data.  The tuple backing this NValue may
                // change, so we need to allocate a copy of the data
                // for the value stored in the unordered set to remain
                // valid.
                NValue newval = val;
                vassert(m_memoryPool != nullptr);
                newval.allocateObjectFromPool(m_memoryPool);
                insert(newval);
            } else {
                insert(val);
            }
            return false; // Include value just this once.
        }
        return true; // Never again this value;
    }
};

/**
 * Mix-in class to tweak some Aggs' behavior when the DISTINCT flag was NOT specified,
 * It "does nothing", by-passing the tracking and de-duping of repeated input values.
 * It is specified as a parameter class that determines the type of the ifDistinct data member.
 */
struct NotDistinct {
    // Pool argument is provided only so
    // interface matches Distinct, above.
    explicit NotDistinct(Pool*) { }
    void clear() { }
    bool excludeValue(const NValue& val) {
        return false; // Include value any number of times
    }
};

// Parameter D is either Distinct of NotDistinct.
template<typename D>
class SumAgg : public Agg {
    D ifDistinct;
public:
    // We're providing a NULL pool argument here to ifDistinct because
    // SUM only operates on numeric values which don't have the same
    // issues as inlined strings.
    explicit SumAgg() : ifDistinct{nullptr} {}

    void advance(const NValue& val) override {
        if (val.isNull() || ifDistinct.excludeValue(val)) {
            return;
        } else if (!m_haveAdvanced) {
            m_value = val;
            m_haveAdvanced = true;
        } else {
            m_value = m_value.op_add(val);
        }
    }

    NValue finalize(ValueType type) override {
        ifDistinct.clear();
        return Agg::finalize(type);
    }
};


// Parameter D is either Distinct of NotDistinct.
template<typename D>
class AvgAgg : public Agg {
    D ifDistinct;
    int64_t m_count = 0;
public:
    // We're providing a NULL pool argument here to ifDistinct because
    // AVG only operates on numeric values which don't have the same
    // issues as inlined strings.
    explicit AvgAgg() : ifDistinct{nullptr} {}

    void advance(const NValue& val) override {
        if (! val.isNull() && ! ifDistinct.excludeValue(val)) {
            if (m_count == 0) {
                m_value = val;
            } else {
                m_value = m_value.op_add(val);
            }
            ++m_count;
        }
    }

    NValue finalize(ValueType type) override {
        if (m_count == 0) {
            return ValueFactory::getNullValue().castAs(type);;
        } else {
            ifDistinct.clear();
            return m_value.op_divide(ValueFactory::getBigIntValue(m_count)).castAs(type);
        }
    }

    void resetAgg() override {
        m_haveAdvanced = false;
        m_count = 0;
    }
};

//count always holds integer
// Parameter D is either Distinct of NotDistinct.
template<typename D>
class CountAgg : public Agg {
    D ifDistinct;
    int64_t m_count = 0;
public:
    CountAgg(Pool* memoryPool) : ifDistinct(memoryPool) {}

    void advance(const NValue& val) override {
        if (! val.isNull() && !ifDistinct.excludeValue(val)) {
            m_count++;
        }
    }

    NValue finalize(ValueType type) override {
        ifDistinct.clear();
        return ValueFactory::getBigIntValue(m_count).castAs(type);
    }

    void resetAgg() override {
        m_haveAdvanced = false;
        m_count = 0;
    }
};

class CountStarAgg : public Agg {
    int64_t m_count = 0;
public:
    CountStarAgg() : m_count(0) {}
    void advance(const NValue& val) override {
        ++m_count;
    }
    NValue finalize(ValueType type) override {
        return ValueFactory::getBigIntValue(m_count).castAs(type);
    }
    void resetAgg() override {
        m_haveAdvanced = false;
        m_count = 0;
    }
};

enum class MinMaxType {
    MIN, MAX
};

template<MinMaxType>
class MinMaxAgg : public Agg {
    Pool* m_memoryPool;
    inline void update(NValue const& val) noexcept;
public:
    MinMaxAgg(Pool* memoryPool) : m_memoryPool(memoryPool) {}
    void advance(const NValue& val) override {
        if (val.isNull()) {
            return;
        } else if (!m_haveAdvanced) {
            m_value = val;
            if (m_value.getVolatile()) {
                // In serial aggregation, the NValue may be backed by
                // a row that is reused and updated for each row
                // produced by a child node.  Because NValue's copy
                // constructor only does a shallow copy, this can lead
                // wrong answers when the Agg's NValue changes
                // unexpectedly.  To avoid this, copy the
                // incoming NValue to its own storage.
                m_value.allocateObjectFromPool(m_memoryPool);
                m_inlineCopiedToNonInline = true;
            }
            m_haveAdvanced = true;
        } else {
            update(val);
            if (m_value.getVolatile()) {
                m_value.allocateObjectFromPool(m_memoryPool);
            }
        }
    }
    NValue finalize(ValueType type) override {
        m_value.castAs(type);
        if (m_inlineCopiedToNonInline) {
            m_value.allocateObjectFromPool();
        }
        return m_value;
    }
};

template<> void MinMaxAgg<MinMaxType::MIN>::update(NValue const& val) noexcept {
    m_value = m_value.op_min(val);
}

template<> void MinMaxAgg<MinMaxType::MAX>::update(NValue const& val) noexcept {
    m_value = m_value.op_max(val);
}

class ApproxCountDistinctAgg : public Agg {
    hll::HyperLogLog m_hyperLogLog {REGISTER_BIT_WIDTH};
protected:
    // Setting this value higher makes for a more accurate
    // estimate but means that the hyperloglogs sent to the
    // coordinator from each partition will be larger.
    //
    // This value is called "b" in the hyperloglog code
    // and papers.  Size of the hyperloglog will be
    // 2^b + 1 bytes.
    //
    // For the version of hyperloglog we use in VoltDB, the max
    // value allowed for b is 16, so the hyperloglogs sent to the
    // coordinator will be 65537 bytes apiece, which seems
    // reasonable.
    static uint8_t const REGISTER_BIT_WIDTH = 16;
    hll::HyperLogLog& hyperLogLog() {
        return m_hyperLogLog;
    }
public:
    ApproxCountDistinctAgg() = default;
    void advance(const NValue& val) override {
        if (! val.isNull()) {
            // Cannot (yet?) handle variable length types.  This should be
            // enforced by the front end, so we don't actually expect this
            // error.
            //
            // FLOATs are not handled due to the possibility of different
            // bit patterns representing the same value (positive/negative
            // zero, and [de-]normalized numbers).  This is also enforced
            // in the front end.
            vassert(! isVariableLengthType(ValuePeeker::peekValueType(val))
                    && ValuePeeker::peekValueType(val) != ValueType::tPOINT
                    && ValuePeeker::peekValueType(val) != ValueType::tDOUBLE);

            int32_t valLength = 0;
            const char* data = ValuePeeker::peekPointerToDataBytes(val, &valLength);
            vassert(valLength != 0);
            m_hyperLogLog.add(data, static_cast<uint32_t>(valLength));
        }
    }
    NValue finalize(ValueType type) override {
        double estimate = m_hyperLogLog.estimate();
        estimate = ::round(estimate); // round to nearest integer
        m_value = ValueFactory::getBigIntValue(static_cast<int64_t>(estimate));
        return m_value;
    }
    void resetAgg() override {
        hyperLogLog().clear();
        Agg::resetAgg();
    }
};

/// When APPROX_COUNT_DISTINCT is split across two fragments of a
/// plan, this agg represents the bottom half of the agg.  It's
/// advance method is inherited from the super class, but it's
/// finalize method produces a serialized hyperloglog to be accepted
/// by a HYPERLOGLOGS_TO_CARD agg on the coordinator.
class ValsToHyperLogLogAgg : public ApproxCountDistinctAgg {
public:
    NValue finalize(ValueType type) override {
        vassert(type == ValueType::tVARBINARY);
        // serialize the hyperloglog as varbinary, to send to
        // coordinator.
        //
        // TODO: We're doing a fair bit of copying here, first to the
        // string stream, then to the temp varbinary object.  We could
        // get away with just one copy here.
        std::ostringstream oss;
        hyperLogLog().dump(oss);
        return ValueFactory::getTempBinaryValue(oss.str().c_str(),
                static_cast<int32_t>(oss.str().length()));
    }
};

/// When APPROX_COUNT_DISTINCT is split across two fragments of a
/// plan, this agg represents the top half of the agg.  It's finalize
/// method is inherited from the super class, but it's advance method
/// accepts serialized hyperloglogs from each partition.
class HyperLogLogsToCardAgg : public ApproxCountDistinctAgg {
public:
    void advance(const NValue& val) override {
        vassert(ValuePeeker::peekValueType(val) == ValueType::tVARBINARY);
        vassert(!val.isNull());

        // TODO: we're doing some unnecessary copying here to
        // deserialize the hyperloglog and merge it with the
        // agg's HLL instance.

        int32_t length;
        const char* buf = ValuePeeker::peekObject_withoutNull(val, &length);
        vassert(length > 0);
        std::istringstream iss(std::string(buf, length));
        hll::HyperLogLog distHll(REGISTER_BIT_WIDTH);
        distHll.restore(iss);
        hyperLogLog().merge(distHll);
    }
};

// User-defined aggregate function
class UserDefineAgg : public Agg {
    VoltDBEngine* m_engine = ExecutorContext::getExecutorContext()->getEngine();
    int m_functionId;
    int m_udafIndex;
    bool m_isWorker;      // worker or coordinator
    bool m_isPartition;   // partitioned table or replicated table

    // Used for vectorization of assemble method
    std::vector<NValue> m_argVector{ROWS_PER_BATCH, NValue()};
    int m_argCount = 0;

    // NOTE: maximum size of of argument vector,
    // declared as static variable shared by all instances of the class.
    // This is how many rows we process each time.
    static const int ROWS_PER_BATCH = 32;
public:
    UserDefineAgg(int id, bool isWorker, bool isPartition, int udafIndex) :
        m_functionId(id), m_udafIndex(udafIndex), m_isWorker(isWorker), m_isPartition(isPartition) {
        m_engine->callJavaUserDefinedAggregateStart(m_functionId);
    }

    void advance(const NValue& val) override {
        // if this is a worker, we will need to call the assemble method to accumulate
        // the values within this partition
        if (m_isWorker) {
            // Add the argument (val) to the argument vector
            // When the argument vector is full (i.e., argument size equals max size),
            // call the assmble method in udaf, and pass the argument vector and argument count to it
            m_argVector[m_argCount++] = val; // NValue assignment operator overloading
            m_argCount %= ROWS_PER_BATCH;
            if (m_argCount == 0) {
                m_engine->callJavaUserDefinedAggregateAssemble(
                        m_functionId, m_argVector, ROWS_PER_BATCH, m_udafIndex);
            }
        } else {
            // if this is a coordinator (not worker), we will need to call the combine method
            // to deserialize the byte arrays from other partitions and merge them
            m_engine->callJavaUserDefinedAggregateCombine(m_functionId, val, m_udafIndex);
        }
    }

    NValue finalize(ValueType type) override {
        // Check whether there are arguments stored in argument vector while the argument is not full
        // If so, call the assemble method in udaf, and pass the argument vector and actual argument count to it
        if (m_argCount > 0) {
            m_engine->callJavaUserDefinedAggregateAssemble(
                    m_functionId, m_argVector, m_argCount, m_udafIndex);
            m_argCount = 0;
        }
        if (m_isPartition && m_isWorker) {
            // if this is a partitioned table and a worker, we will call the worker end method
            // to serialize the instance to a byte array and send it to the coordinator
            return m_engine->callJavaUserDefinedAggregateWorkerEnd(m_functionId, m_udafIndex);
        } else {
            // if this is not a partitioned table which means this is a replicated table, or this is
            // a coordinator (not worker), we are ready to return the final result by calling the
            // coordinator end method
            return m_engine->callJavaUserDefinedAggregateCoordinatorEnd(m_functionId, m_udafIndex);
        }
    }

    void resetAgg() override {
        m_engine->callJavaUserDefinedAggregateStart(m_functionId);
    }
};

/*
 * Create an instance of an aggregator for the specified aggregate type and "distinct" flag.
 * The object is allocated from the provided memory pool.
 */
inline Agg* getAggInstance(Pool& memoryPool, ExpressionType aggType, bool isDistinct) {
    switch (aggType) {
        case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
            return new (memoryPool) CountStarAgg();
        case EXPRESSION_TYPE_AGGREGATE_MIN:
            return new (memoryPool) MinMaxAgg<MinMaxType::MIN>(&memoryPool);
        case EXPRESSION_TYPE_AGGREGATE_MAX:
            return new (memoryPool) MinMaxAgg<MinMaxType::MAX>(&memoryPool);
        case EXPRESSION_TYPE_AGGREGATE_COUNT:
            if (isDistinct) {
                return new (memoryPool) CountAgg<Distinct>(&memoryPool);
            } else {
                return new (memoryPool) CountAgg<NotDistinct>(&memoryPool);
            }
        case EXPRESSION_TYPE_AGGREGATE_SUM:
            if (isDistinct) {
                return new (memoryPool) SumAgg<Distinct>();
            } else {
                return new (memoryPool) SumAgg<NotDistinct>();
            }
        case EXPRESSION_TYPE_AGGREGATE_AVG:
            if (isDistinct) {
                return new (memoryPool) AvgAgg<Distinct>();
            } else {
                return new (memoryPool) AvgAgg<NotDistinct>();
            }
        case EXPRESSION_TYPE_AGGREGATE_APPROX_COUNT_DISTINCT:
            return new (memoryPool) ApproxCountDistinctAgg();
        case EXPRESSION_TYPE_AGGREGATE_VALS_TO_HYPERLOGLOG:
            return new (memoryPool) ValsToHyperLogLogAgg();
        case EXPRESSION_TYPE_AGGREGATE_HYPERLOGLOGS_TO_CARD:
            return new (memoryPool) HyperLogLogsToCardAgg();
        default:
            throwSerializableTypedEEException(
                    VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                    "Unknown aggregate type %d", aggType);
    }
}

inline Agg* getUDAFAggInstance(Pool& memoryPool, int functionId,
        bool isWorker, bool isPartition, int udafIndex) {
    return new (memoryPool) UserDefineAgg(functionId, isWorker, isPartition, udafIndex);
}


/**
 * Aggregate Executor Base
 */
bool AggregateExecutorBase::p_init(AbstractPlanNode*, const ExecutorVector& executorVector) {
    AggregatePlanNode* node = dynamic_cast<AggregatePlanNode*>(m_abstractNode);
    vassert(node);

    m_inputExpressions = node->getAggregateInputExpressions();
    for (int i = 0; i < m_inputExpressions.size(); i++) {
        VOLT_DEBUG("AGG INPUT EXPRESSION[%d]: %s", i,
                   m_inputExpressions[i] ? m_inputExpressions[i]->debug().c_str() : "null");
    }

    /*
     * Find the difference between the set of aggregate output columns
     * (output columns resulting from an aggregate) and output columns.
     * Columns that are not the result of aggregates are being passed
     * through from the input table. Do this extra work here rather then
     * serialize yet more data.
     */
    std::vector<bool> outputColumnsResultingFromAggregates(node->getOutputSchema().size(), false);
    m_aggregateOutputColumns = node->getAggregateOutputColumns();
    for(int aOC : m_aggregateOutputColumns) {
        outputColumnsResultingFromAggregates[aOC] = true;
    }
    for (int ii = 0; ii < outputColumnsResultingFromAggregates.size(); ii++) {
        if (!outputColumnsResultingFromAggregates[ii]) {
            m_passThroughColumns.push_back(ii);
        }
    }

    if (!node->isInline()) {
        setTempOutputTable(executorVector);
    }
    m_partialSerialGroupByColumns = node->getPartialGroupByColumns();

    m_aggTypes = node->getAggregates();
    m_aggregateIds = node->getAggregateIds();
    m_distinctAggs = node->getDistinctAggregates();
    m_isWorker = node->getIsWorker();
    m_isPartition = node->getPartition();
    m_groupByExpressions = node->getGroupByExpressions();
    node->collectOutputExpressions(m_outputColumnExpressions);

    // m_passThroughColumns.size() == m_groupByExpressions.size() is not true,
    // Because group by unique column may be able to select other columns
    m_prePredicate = node->getPrePredicate();
    m_postPredicate = node->getPostPredicate();

    m_groupByKeySchema = constructGroupBySchema(false);
    m_groupByKeyPartialHashSchema = nullptr;
    if (! m_partialSerialGroupByColumns.empty()) {
        for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
            if (std::find(m_partialSerialGroupByColumns.begin(),
                          m_partialSerialGroupByColumns.end(), ii)
                == m_partialSerialGroupByColumns.end()) {
                // Find the partial hash group by columns
                m_partialHashGroupByColumns.push_back(ii);;
            }
        }
        m_groupByKeyPartialHashSchema = constructGroupBySchema(true);
    }

    return true;
}

inline TupleSchema* AggregateExecutorBase::constructGroupBySchema(bool partial) {
    std::vector<ValueType> groupByColumnTypes;
    std::vector<int32_t> groupByColumnSizes;
    std::vector<bool> groupByColumnAllowNull;
    std::vector<bool> groupByColumnInBytes;

    if (partial) {
        for (int gbIdx : m_partialHashGroupByColumns) {
            AbstractExpression* expr = m_groupByExpressions[gbIdx];
            groupByColumnTypes.push_back(expr->getValueType());
            groupByColumnSizes.push_back(expr->getValueSize());
            groupByColumnAllowNull.push_back(true);
            groupByColumnInBytes.push_back(expr->getInBytes());
        }
    } else {
        for (AbstractExpression* expr : m_groupByExpressions) {
            groupByColumnTypes.push_back(expr->getValueType());
            groupByColumnSizes.push_back(expr->getValueSize());
            groupByColumnAllowNull.push_back(true);
            groupByColumnInBytes.push_back(expr->getInBytes());
        }
    }
    return TupleSchema::createTupleSchema(
            groupByColumnTypes, groupByColumnSizes,
            groupByColumnAllowNull, groupByColumnInBytes);
}

inline void AggregateExecutorBase::initCountingPredicate(const NValueArray& params,
        CountingPostfilter* parentPostfilter) {
    VOLT_DEBUG("started AGGREGATE");
    vassert(dynamic_cast<AggregatePlanNode*>(m_abstractNode));
    vassert(m_tmpOutputTable);
    //
    // OPTIMIZATION: NESTED LIMIT for serial aggregation
    //
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    auto* inlineLimitNode = dynamic_cast<LimitPlanNode*>(
            m_abstractNode->getInlinePlanNode(PlanNodeType::Limit));
    if (inlineLimitNode) {
        std::tie(limit, offset) = inlineLimitNode->getLimitAndOffset(params);
    }
    m_postfilter = CountingPostfilter(m_tmpOutputTable, m_postPredicate, limit, offset, parentPostfilter);
}

/// Helper method responsible for inserting the results of the
/// aggregation into a new tuple in the output table as well as passing
/// through any additional columns from the input table.
inline bool AggregateExecutorBase::insertOutputTuple(AggregateRow* aggregateRow) {
    if (!m_postfilter.isUnderLimit()) {
        return false;
    }

    TableTuple& tempTuple = m_tmpOutputTable->tempTuple();

    // This first pass is to add all columns that were aggregated on.
    Agg** aggs = aggregateRow->m_aggregates;
    for (int ii = 0; ii < m_aggregateOutputColumns.size(); ii++) {
        const int columnIndex = m_aggregateOutputColumns[ii];
        NValue result = aggs[ii]->finalize(tempTuple.getSchema()->columnType(columnIndex));
        tempTuple.setNValue(columnIndex, result);
    }

    VOLT_TRACE("Setting passthrough columns");
    for(int output_col_index : m_passThroughColumns) {
        tempTuple.setNValue(output_col_index,
                            m_outputColumnExpressions[output_col_index]->eval(
                                &aggregateRow->m_passThroughTuple));
    }

    bool needInsert = m_postfilter.eval(&tempTuple, nullptr);
    if (needInsert) {
        m_tmpOutputTable->insertTempTuple(tempTuple);
    }

    VOLT_TRACE("output_table:\n%s", m_tmpOutputTable->debug().c_str());
    return needInsert;
}

inline void AggregateExecutorBase::advanceAggs(AggregateRow* aggregateRow, const TableTuple& tuple) {
    Agg** aggs = aggregateRow->m_aggregates;
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        // In particular, COUNT(*) accepts a dummy NValue from a NULL input expression.
        AbstractExpression* inputExpr = m_inputExpressions[ii];
        aggs[ii]->advance(inputExpr ? inputExpr->eval(&tuple) : NValue());
    }
}

/*
 * Create an instance of an aggregator for the specified aggregate type.
 * The object is constructed in memory from the provided memory pool.
 */
inline void AggregateExecutorBase::initAggInstances(AggregateRow* aggregateRow) {
    Agg** aggs = aggregateRow->m_aggregates;
    std::unordered_map<int, int> udafIndexes;
    // UDFTODO: If you make the change in AggregatePlanNode, you will need another index
    // to track the id for udaf
    /*
        int udafJsonIndex = 0;
        for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        if (m_aggTypes[ii] == EXPRESSION_TYPE_USER_DEFINED_AGGREGATE) {
            aggs[ii] = getUDAFAggInstance(m_memoryPool, m_aggregateIds[udafJsonIndex], m_isWorker[udafJsonIndex], m_isPartition[udafJsonIndex], udafIndexes[m_aggregateIds[udafJsonIndex]]++);
            udafJsonIndex++;
        }
        else {
            aggs[ii] = getAggInstance(m_memoryPool, m_aggTypes[ii], m_distinctAggs[ii]);
        }
    }
    */
    for (int ii = 0; ii < m_aggTypes.size(); ii++) {
        if (m_aggTypes[ii] == EXPRESSION_TYPE_USER_DEFINED_AGGREGATE) {
            aggs[ii] = getUDAFAggInstance(m_memoryPool, m_aggregateIds[ii],
                    m_isWorker[ii], m_isPartition[ii], udafIndexes[m_aggregateIds[ii]]++);
        } else {
            aggs[ii] = getAggInstance(m_memoryPool, m_aggTypes[ii], m_distinctAggs[ii]);
        }
    }
}

void AggregateExecutorBase::initGroupByKeyTuple(const TableTuple& nextTuple) {
    TableTuple& nextGroupByKeyTuple = m_nextGroupByKeyStorage;
    if (nextGroupByKeyTuple.isNullTuple()) {
        // Tuple spaces got allocated
        m_nextGroupByKeyStorage.allocateActiveTuple();
    }
    // TODO: Here is where an inline projection executor could be used to initialize both a group key tuple
    // and an agg input tuple from the same raw input tuple.
    // configure a tuple
    for (int ii = 0; ii < m_groupByExpressions.size(); ii++) {
        nextGroupByKeyTuple.setNValue(ii, m_groupByExpressions[ii]->eval(&nextTuple));
    }
}

TableTuple& AggregateExecutorBase::swapWithInprogressGroupByKeyTuple() {
    TableTuple& nextGroupByKeyTuple = m_nextGroupByKeyStorage;
    void* recycledStorage = m_inProgressGroupByKeyTuple.address();
    void* inProgressStorage = nextGroupByKeyTuple.address();
    m_inProgressGroupByKeyTuple.move(inProgressStorage);
    nextGroupByKeyTuple.move(recycledStorage);
    return nextGroupByKeyTuple;
}

TableTuple AggregateExecutorBase::p_execute_init(
        const NValueArray& params, ProgressMonitorProxy* pmp,
        const TupleSchema * schema, AbstractTempTable* newTempTable,
        CountingPostfilter* parentPostfilter) {
    if (newTempTable != nullptr) {
        m_tmpOutputTable = newTempTable;
    }
    m_memoryPool.purge();
    initCountingPredicate(params, parentPostfilter);
    m_pmp = pmp;

    m_nextGroupByKeyStorage.init(m_groupByKeySchema, &m_memoryPool);
    TableTuple& nextGroupByKeyTuple = m_nextGroupByKeyStorage;
    nextGroupByKeyTuple.move(nullptr);

    m_inputSchema = schema;

    m_inProgressGroupByKeyTuple.setSchema(m_groupByKeySchema);
    // set the schema first because of the NON-null check in MOVE function
    m_inProgressGroupByKeyTuple.move(nullptr);

    char* storage = reinterpret_cast<char*>(m_memoryPool.allocateZeroes(
                schema->tupleLength() + TUPLE_HEADER_SIZE));
    return TableTuple(storage, schema);
}

void AggregateExecutorBase::p_execute_finish() {
    TableTuple& nextGroupByKeyTuple = m_nextGroupByKeyStorage;
    nextGroupByKeyTuple.move(nullptr);
    m_inProgressGroupByKeyTuple.move(nullptr);
    m_memoryPool.purge();
}

/**
 * Aggregate Hash Executor
 */

AggregateHashExecutor::~AggregateHashExecutor() {}

TableTuple AggregateHashExecutor::p_execute_init(
        const NValueArray& params, ProgressMonitorProxy* pmp,
        const TupleSchema * schema, AbstractTempTable* newTempTable,
        CountingPostfilter* parentPostfilter) {
    VOLT_TRACE("hash aggregate executor init..");
    m_hash.clear();
    return AggregateExecutorBase::p_execute_init(params, pmp, schema, newTempTable, parentPostfilter);
}

bool AggregateHashExecutor::p_execute(const NValueArray& params) {
    // Input table
    Table* input_table = m_abstractNode->getInputTable();
    vassert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());

    const TupleSchema * inputSchema = input_table->schema();
    vassert(inputSchema);
    TableIterator it = input_table->iteratorDeletingAsWeGo();
    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);

    TableTuple nextTuple = p_execute_init(params, &pmp, inputSchema, nullptr);

    VOLT_TRACE("looping..");
    while (it.next(nextTuple)) {
        vassert(m_postfilter.isUnderLimit()); // hash aggregation can not early return for limit
        p_execute_tuple(nextTuple);
    }
    p_execute_finish();

    return true;
}

void AggregateHashExecutor::p_execute_tuple(const TableTuple& nextTuple) {
    m_pmp->countdownProgress();
    initGroupByKeyTuple(nextTuple);
    AggregateRow* aggregateRow;
    TableTuple& nextGroupByKeyTuple = m_nextGroupByKeyStorage;
    // Search for the matching group.
    HashAggregateMapType::const_iterator keyIter = m_hash.find(nextGroupByKeyTuple);

    // Group not found. Make a new entry in the hash for this new group.
    if (keyIter == m_hash.end()) {
        VOLT_TRACE("hash aggregate: new group..");
        aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();
        m_hash.insert(HashAggregateMapType::value_type(nextGroupByKeyTuple, aggregateRow));

        initAggInstances(aggregateRow);

        char* storage = reinterpret_cast<char*>(m_memoryPool.allocateZeroes(
                    m_inputSchema->tupleLength() + TUPLE_HEADER_SIZE));
        TableTuple passThroughTupleSource = TableTuple(storage, m_inputSchema);

        aggregateRow->recordPassThroughTuple(passThroughTupleSource, nextTuple);
        // The map is referencing the current key tuple for use by the new group,
        // so force a new tuple allocation to hold the next candidate key.
        nextGroupByKeyTuple.move(nullptr);

        if (m_aggTypes.empty()) {
            insertOutputTuple(aggregateRow);
            return;
        }
    } else {
        // otherwise, the agg row is the second item of the pair...
        aggregateRow = keyIter->second;
    }
    // update the aggregation calculation.
    advanceAggs(aggregateRow, nextTuple);
}

void AggregateHashExecutor::p_execute_finish() {
    VOLT_TRACE("finalizing..");

    // If there is no aggregation, results are already inserted already
    if (! m_aggTypes.empty()) {
        for (auto iter : m_hash) {
            AggregateRow* aggregateRow = iter.second;
            if (insertOutputTuple(aggregateRow)) {
                m_pmp->countdownProgress();
            }
            delete aggregateRow;
        }
    }

    // Clean up
    m_hash.clear();
    AggregateExecutorBase::p_execute_finish();
}

/**
 * Aggregate Serial Executor
 */

AggregateSerialExecutor::~AggregateSerialExecutor() {}

TableTuple AggregateSerialExecutor::p_execute_init(
        const NValueArray& params, ProgressMonitorProxy* pmp,
        const TupleSchema * schema, AbstractTempTable* newTempTable,
        CountingPostfilter* parentPostfilter) {
    VOLT_TRACE("serial aggregate executor init..");
    TableTuple nextInputTuple = AggregateExecutorBase::p_execute_init(
            params, pmp, schema, newTempTable, parentPostfilter);

    m_aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();
    m_noInputRows = true;
    m_failPrePredicateOnFirstRow = false;

    char* storage = reinterpret_cast<char*>(m_memoryPool.allocateZeroes(
                schema->tupleLength() + TUPLE_HEADER_SIZE));
    m_passThroughTupleSource = TableTuple(storage, schema);

    // for next input tuple
    return nextInputTuple;
}

bool AggregateSerialExecutor::p_execute(const NValueArray& params) {
    // Input table
    Table* input_table = m_abstractNode->getInputTable();
    vassert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());
    TableIterator it = input_table->iteratorDeletingAsWeGo();
    TableTuple nextTuple(input_table->schema());

    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
    p_execute_init(params, &pmp, input_table->schema(), nullptr);

    while (m_postfilter.isUnderLimit() && it.next(nextTuple)) {
        m_pmp->countdownProgress();
        p_execute_tuple(nextTuple);
    }
    p_execute_finish();
    VOLT_TRACE("finalizing..");
    return true;
}

void AggregateSerialExecutor::p_execute_tuple(const TableTuple& nextTuple) {
    // Use the first input tuple to "prime" the system.
    if (m_noInputRows) {
        // ENG-1565: for this special case, can have only one input row, apply the predicate here
        if (m_prePredicate == nullptr || m_prePredicate->eval(&nextTuple, nullptr).isTrue()) {
            initGroupByKeyTuple(nextTuple);

            // Start the aggregation calculation.
            initAggInstances(m_aggregateRow);
            m_aggregateRow->recordPassThroughTuple(m_passThroughTupleSource, nextTuple);
            advanceAggs(m_aggregateRow, nextTuple);
        } else {
            m_failPrePredicateOnFirstRow = true;
        }
        m_noInputRows = false;
        return;
    }

    TableTuple& nextGroupByKeyTuple = swapWithInprogressGroupByKeyTuple();

    initGroupByKeyTuple(nextTuple);

    for (int ii = m_groupByKeySchema->columnCount() - 1; ii >= 0; --ii) {
        if (nextGroupByKeyTuple.getNValue(ii) != m_inProgressGroupByKeyTuple.getNValue(ii)) {
            VOLT_TRACE("new group!");
            // Output old row.
            if (insertOutputTuple(m_aggregateRow)) {
                m_pmp->countdownProgress();
            }
            m_aggregateRow->resetAggs();

            // record the new group scanned tuple
            m_aggregateRow->recordPassThroughTuple(m_passThroughTupleSource, nextTuple);
            break;
        }
    }
    // update the aggregation calculation.
    advanceAggs(m_aggregateRow, nextTuple);
}

void AggregateSerialExecutor::p_execute_finish() {
    if (m_postfilter.isUnderLimit()) {
        if (m_noInputRows || m_failPrePredicateOnFirstRow) {
            VOLT_TRACE("finalizing after no input rows..");
            // No input rows means either no group rows (when grouping) or an empty table row (otherwise).
            // Note the difference between these two cases:
            //   SELECT SUM(A) FROM BBB,            when BBB has no tuple, produces one output row.
            //   SELECT SUM(A) FROM BBB GROUP BY C, when BBB has no tuple, produces no output row.
            if (m_groupByKeySchema->columnCount() == 0) {
                VOLT_TRACE("no input row, but output an empty result row for the whole table.");
                initAggInstances(m_aggregateRow);
                if (insertOutputTuple(m_aggregateRow)) {
                    m_pmp->countdownProgress();
                }
            }
        } else if (insertOutputTuple(m_aggregateRow)) {
            // There's one last group (or table) row in progress that needs to be output.
            m_pmp->countdownProgress();
        }
    }

    // clean up the member variables
    delete m_aggregateRow;
    AggregateExecutorBase::p_execute_finish();
}

/**
 * Aggregate Partial Executor
 */
AggregatePartialExecutor::~AggregatePartialExecutor() {}

TableTuple AggregatePartialExecutor::p_execute_init(
        const NValueArray& params, ProgressMonitorProxy* pmp,
        const TupleSchema * schema, AbstractTempTable* newTempTable,
        CountingPostfilter* parentPostfilter) {
    VOLT_TRACE("partial aggregate executor init..");
    TableTuple nextInputTuple = AggregateExecutorBase::p_execute_init(
            params, pmp, schema, newTempTable, parentPostfilter);
    m_atTheFirstRow = true;
    m_nextPartialGroupByKeyStorage.init(m_groupByKeyPartialHashSchema, &m_memoryPool);
    TableTuple& nextPartialGroupByKeyTuple = m_nextGroupByKeyStorage;
    nextPartialGroupByKeyTuple.move(nullptr);

    m_hash.clear();

    // for next input tuple
    return nextInputTuple;
}

bool AggregatePartialExecutor::p_execute(const NValueArray& params) {
    // Input table
    Table* input_table = m_abstractNode->getInputTable(0);
    vassert(input_table);
    VOLT_TRACE("input table\n%s", input_table->debug().c_str());
    TableIterator it = input_table->iteratorDeletingAsWeGo();
    TableTuple nextTuple(input_table->schema());

    ProgressMonitorProxy pmp(m_engine->getExecutorContext(), this);
    p_execute_init(params, &pmp, input_table->schema(), nullptr);

    while (m_postfilter.isUnderLimit() && it.next(nextTuple)) {
        m_pmp->countdownProgress();
        p_execute_tuple(nextTuple);
    }
    p_execute_finish();
    VOLT_TRACE("finalizing..");

    return true;
}

inline void AggregatePartialExecutor::initPartialHashGroupByKeyTuple(const TableTuple& nextTuple) {
    TableTuple& nextGroupByKeyTuple = m_nextPartialGroupByKeyStorage;
    if (nextGroupByKeyTuple.isNullTuple()) {
        m_nextPartialGroupByKeyStorage.allocateActiveTuple();
    }
    for (int ii = 0; ii < m_partialHashGroupByColumns.size(); ii++) {
        AbstractExpression* expr = m_groupByExpressions[m_partialHashGroupByColumns.at(ii)];
        nextGroupByKeyTuple.setNValue(ii, expr->eval(&nextTuple));
    }
}

void AggregatePartialExecutor::p_execute_tuple(const TableTuple& nextTuple) {
    TableTuple& nextGroupByKeyTuple = swapWithInprogressGroupByKeyTuple();

    initGroupByKeyTuple(nextTuple);

    for(int ii : m_partialSerialGroupByColumns) {
        if (m_atTheFirstRow ||
            nextGroupByKeyTuple.getNValue(ii) != m_inProgressGroupByKeyTuple.getNValue(ii)) {

            VOLT_TRACE("new group!");
            m_atTheFirstRow = false;

            // Output old group rows.
            for (auto& iter : m_hash) {
                AggregateRow *aggregateRow = iter.second;
                if (insertOutputTuple(aggregateRow)) {
                    m_pmp->countdownProgress();
                }
                delete aggregateRow;
            }

            // clean up the partial hash aggregate.
            m_hash.clear();
            break;
        }
    }

    // Hash aggregate on the rest of group by expressions.
    initPartialHashGroupByKeyTuple(nextTuple);
    AggregateRow* aggregateRow;
    TableTuple& nextPartialGroupByKeyTuple = m_nextPartialGroupByKeyStorage;
    auto const keyIter = m_hash.find(nextPartialGroupByKeyTuple);

    // Group not found. Make a new entry in the hash for this new group.
    if (keyIter == m_hash.end()) {
        VOLT_TRACE("partial hash aggregate: new sub group..");
        aggregateRow = new (m_memoryPool, m_aggTypes.size()) AggregateRow();
        m_hash.insert(HashAggregateMapType::value_type(nextPartialGroupByKeyTuple, aggregateRow));
        initAggInstances(aggregateRow);

        char* storage = reinterpret_cast<char*>(
                m_memoryPool.allocateZeroes(m_inputSchema->tupleLength() + TUPLE_HEADER_SIZE));
        TableTuple passThroughTupleSource = TableTuple (storage, m_inputSchema);
        aggregateRow->recordPassThroughTuple(passThroughTupleSource, nextTuple);
        // The map is referencing the current key tuple for use by the new group,
        // so force a new tuple allocation to hold the next candidate key.
        nextPartialGroupByKeyTuple.move(nullptr);
    } else {
        // otherwise, the agg row is the second item of the pair...
        aggregateRow = keyIter->second;
    }

    // update the aggregation calculation.
    advanceAggs(aggregateRow, nextTuple);
}
// TODO: Refactoring the last half of the above function with HASH aggregation

void AggregatePartialExecutor::p_execute_finish() {
    VOLT_TRACE("finalizing..");
    for (auto const& iter : m_hash) {
        AggregateRow *aggregateRow = iter.second;
        if (insertOutputTuple(aggregateRow)) {
            m_pmp->countdownProgress();
        }
        delete aggregateRow;
    }
    // Clean up
    m_hash.clear();
    TableTuple& nextGroupByKeyTuple = m_nextPartialGroupByKeyStorage;
    nextGroupByKeyTuple.move(nullptr);
    AggregateExecutorBase::p_execute_finish();
}
}
