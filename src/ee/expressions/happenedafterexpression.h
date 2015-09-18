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
#ifndef VOLTDBHAPPENEDAFTEREXPRESSION_H
#define VOLTDBHAPPENEDAFTEREXPRESSION_H

#include "expressions/abstractexpression.h"
#include "common/tabletuple.h"

namespace voltdb {

class HappenedAfterExpression : public AbstractExpression {
public:
    HappenedAfterExpression(int32_t clusterId, int64_t timestamp) :
        AbstractExpression(EXPRESSION_TYPE_HAPPENED_AFTER), clusterId(clusterId),
        lastSeenTimestamp(timestamp) {
        VOLT_TRACE("HashRangeExpression %d %d", m_type, value_idx);
    }

    virtual voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert(tuple1);
        if ( ! tuple1 ) {
            throw SerializableEEException(
                    "TupleValueExpression::"
                    "eval:"
                    " Couldn't find tuple 1 (possible index scan planning error)");
        }
        ExecutorContext *ec = ExecutorContext::getExecutorContext();
        NValue hiddenNValue = tuple1->getHiddenNValue(0);
        int64_t timestamp = ec->getTimestampFromHiddenNValue(hiddenNValue);
        int32_t clusterId = ec->getClusterIdFromHiddenNValue(hiddenNValue);

        if (clusterId == this->clusterId && timestamp > lastSeenTimestamp) {
            return NValue::getTrue();
        }

        return NValue::getFalse();
    }

    std::string debugInfo(const std::string &spacer) const {
            std::ostringstream buffer;
            buffer << spacer << "Happened after expression ["
                    << "clusterId " << clusterId
                    << "timestamp " << lastSeenTimestamp
                    << "]" << std::endl;
            return (buffer.str());
        }
private:
    const int32_t clusterId;
    const int64_t lastSeenTimestamp;
};
}

#endif
