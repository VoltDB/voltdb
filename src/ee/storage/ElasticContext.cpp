/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include "storage/ElasticContext.h"
#include "storage/persistenttable.h"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "common/FixUnusedAssertHack.h"
#include "expressions/hashrangeexpression.h"
#include "logging/LogManager.h"
#include <cassert>
#include <sstream>
#include <limits>

namespace voltdb {

ElasticContext::ElasticContext(PersistentTable &table,
                               PersistentTableSurgeon &surgeon,
                               int32_t partitionId,
                               TupleSerializer &serializer,
                               const std::vector<std::string> &predicateStrings,
                               size_t nTuplesPerCall) :
    TableStreamerContext(table, surgeon, partitionId, serializer, predicateStrings),
    m_predicateStrings(predicateStrings), // retained for cloning here, not in TableStreamerContext.
    m_nTuplesPerCall(nTuplesPerCall),
    m_indexActive(false)
{
    if (predicateStrings.size() != 1) {
        throwFatalException("ElasticContext::ElasticContext() expects a single predicate.");
    }
}

ElasticContext::~ElasticContext()
{}

TableStreamerContext* ElasticContext::cloneForTruncatedTable(PersistentTableSurgeon &surgeon)
{
    if ( ! m_indexActive) {
        return NULL;
    }
    ElasticContext *cloned = new ElasticContext(surgeon.getTable(), surgeon,
        getPartitionId(), getSerializer(), m_predicateStrings, m_nTuplesPerCall);
    cloned->handleActivation(TABLE_STREAM_ELASTIC_INDEX);

    TupleOutputStreamProcessor dummyProcessor;
    std::vector<int> dummyPosition;
    while (true) {
        int64_t retCode = cloned->handleStreamMore(dummyProcessor, dummyPosition);
        if (retCode == 0) {
            break;
        } else if (retCode == TABLE_STREAM_SERIALIZATION_ERROR) {
            break;
        } else if (retCode == 1) {
            continue;
        } else {
            char errMsg[1024];
            snprintf(errMsg,
                     1024,
                     "Received an unrecognized return value %jd from handleStreamMore()",
                     (intmax_t)retCode);
            LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, errMsg);
            break;
        }
    }

    return cloned;
}

/**
 * Activation handler.
 */
TableStreamerContext::ActivationReturnCode
ElasticContext::handleActivation(TableStreamType streamType)
{
    // Create the index?
    if (streamType == TABLE_STREAM_ELASTIC_INDEX) {
        // Can't activate an indexing stream during a snapshot.
        if (m_surgeon.hasStreamType(TABLE_STREAM_SNAPSHOT)) {
            LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_WARN,
                "Elastic context activation is not allowed while a snapshot is in progress.");
            return ACTIVATION_FAILED;
        }

        // Allow activation if there is an index, we will check when the predicates
        // are updated to make sure the existing index satisfies the request
        if (m_surgeon.hasIndex()) {
            LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_INFO,
                "Activating elastic index build for index that already exists.");
            return ACTIVATION_SUCCEEDED;
        }
        m_surgeon.createIndex();
        m_scanner.reset(new ElasticScanner(getTable(), m_surgeon.getData()));
        m_indexActive = true;
        return ACTIVATION_SUCCEEDED;
    }

    // Clear the index?
    if (streamType == TABLE_STREAM_ELASTIC_INDEX_CLEAR) {
        if (m_surgeon.hasIndex()) {
            if (!m_surgeon.isIndexEmpty()) {

                std::ostringstream os;
                os << "Elastic index clear is not allowed while an index is "
                   << "present that has not been completely consumed."
                   << std::endl
                   << "Remaining index elements count is "
                   << m_surgeon.indexSize()
                   << std::endl;
                os << "the index contains: " << std::endl;
                const int32_t printUpTo = 1024;
                m_surgeon.printIndex(os, printUpTo);
                if (m_surgeon.indexSize() > printUpTo) {
                    os << "... " << (m_surgeon.indexSize() - printUpTo) << " more elements" << std::endl;
                }
                LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR, os.str().c_str());

                return ACTIVATION_FAILED;
            }
            //Clear the predicates so when we are activated again we won't
            //compare against the old predicate
            m_predicates.clear();
            m_surgeon.dropIndex();
            m_scanner.reset();
            m_indexActive = false;
        }
        return ACTIVATION_SUCCEEDED;
    }

    // It wasn't one of the supported stream types.
    return ACTIVATION_UNSUPPORTED;
}

/**
 * Reactivation handler.
 */
TableStreamerContext::ActivationReturnCode
ElasticContext::handleReactivation(TableStreamType streamType)
{
    return handleActivation(streamType);
}

/**
 * Deactivation handler.
 */
bool ElasticContext::handleDeactivation(TableStreamType streamType)
{
    // Keep this context around to maintain the index.
    return true;
}

/*
 * Serialize to output stream.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t ElasticContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                         std::vector<int> &retPositions)
{
    if (!m_surgeon.hasIndex()) {
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_ERROR,
            "Elastic streaming was invoked without proper activation.");
        return TABLE_STREAM_SERIALIZATION_ERROR;
    }
    if (m_surgeon.isIndexingComplete()) {
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_INFO,
            "Indexing was already complete.");
        return 0;
    }

    // Populate index with current tuples.
    // Table changes are tracked through notifications.
    size_t i = 0;
    TableTuple tuple(getTable().schema());
    while (m_scanner->next(tuple)) {
        if (getPredicates()[0].eval(&tuple).isTrue()) {
            m_surgeon.indexAdd(tuple);
        }
        // Take a breather after every chunk of m_nTuplesPerCall tuples.
        if (++i == m_nTuplesPerCall) {
            break;
        }
    }

    // Done with indexing?
    bool indexingComplete = m_scanner->isScanComplete();
    if (indexingComplete) {
        m_surgeon.setIndexingComplete();
    }
    return indexingComplete ? 0 : 1;
}

/**
 * Tuple insert handler lets us add late arriving tuples to the index.
 */
bool ElasticContext::notifyTupleInsert(TableTuple &tuple)
{
    if (m_indexActive) {
        StreamPredicateList &predicates = getPredicates();
        assert(predicates.size() > 0);
        if (predicates[0].eval(&tuple).isTrue()) {
            m_surgeon.indexAdd(tuple);
        }
    }
    return true;
}

/**
 * Tuple update handler is not currently needed.
 */
bool ElasticContext::notifyTupleUpdate(TableTuple &tuple)
{
    return true;
}

/**
 * Tuple delete handler lets us erase tuples from the index.
 */
bool ElasticContext::notifyTupleDelete(TableTuple &tuple)
{
    if (m_indexActive) {
        if (m_surgeon.indexHas(tuple)) {
            m_surgeon.indexRemove(tuple);
        }
    }
    return true;
}

/**
 * Tuple compaction handler lets us reindex when a tuple's address changes.
 */
void ElasticContext::notifyTupleMovement(TBPtr sourceBlock,
                                         TBPtr targetBlock,
                                         TableTuple &sourceTuple,
                                         TableTuple &targetTuple)
{
    if (m_indexActive) {
        StreamPredicateList &predicates = getPredicates();
        assert(predicates.size() > 0);
        if (m_surgeon.indexHas(sourceTuple)) {
            m_surgeon.indexRemove(sourceTuple);
        }
        if (predicates[0].eval(&targetTuple).isTrue()) {
            m_surgeon.indexAdd(targetTuple);
        }
    }
}

/**
 * Parse and save predicates.
 */
void ElasticContext::updatePredicates(const std::vector<std::string> &predicateStrings) {
    //If there is already a predicate and thus presumably an index, make sure the request is a subset of what exists
    //That should always be the case, but wrong answers will follow if we are wrong
    if (m_predicates.size() > 0 && dynamic_cast<HashRangeExpression*>(&m_predicates[0]) != NULL && predicateStrings.size() > 0) {
        PlannerDomRoot domRoot(predicateStrings[0].c_str());
        if (!domRoot.isNull()) {
            PlannerDomValue predicateObject = domRoot.rootObject();
            HashRangeExpression *expression = dynamic_cast<HashRangeExpression*>(&m_predicates[0]);
            if (predicateObject.hasKey("predicateExpression")) {
                PlannerDomValue predicateExpression = predicateObject.valueForKey("predicateExpression");
                PlannerDomValue rangesArray = predicateExpression.valueForKey("RANGES");
                for (int ii = 0; ii < rangesArray.arrayLen(); ii++) {
                    PlannerDomValue arrayObject = rangesArray.valueAtIndex(ii);
                    PlannerDomValue rangeStartValue = arrayObject.valueForKey("RANGE_START");
                    PlannerDomValue rangeEndValue = arrayObject.valueForKey("RANGE_END");
                    if (!expression->binarySearch(rangeStartValue.asInt()).isTrue()) {
                        throwFatalException("ElasticContext activate failed because a context already existed with conflicting ranges, conflicting range start is %d", rangeStartValue.asInt());
                    }
                    if (!expression->binarySearch(rangeEndValue.asInt()).isTrue()) {
                        throwFatalException("ElasticContext activate failed because a context already existed with conflicting ranges, conflicting range end is %d", rangeStartValue.asInt());
                    }
                }
            }
        }
    }
    m_predicateStrings = predicateStrings; // retain for possible clone after TRUNCATE TABLE
    TableStreamerContext::updatePredicates(predicateStrings);
}

} // namespace voltdb
