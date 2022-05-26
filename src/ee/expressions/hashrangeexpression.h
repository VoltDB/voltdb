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
#ifndef VOLTDBHASHRANGEEXPRESSION_H
#define VOLTDBHASHRANGEEXPRESSION_H

#include "expressions/abstractexpression.h"
#include "common/tabletuple.h"

#include <string>
#include <sstream>
#include <boost/scoped_array.hpp>

namespace voltdb {

typedef std::pair<int32_t, int32_t> srange_type;

class HashRangeExpression : public AbstractExpression {
public:
    HashRangeExpression(int value_idx, srange_type *ranges, int num_ranges )
: AbstractExpression(EXPRESSION_TYPE_HASH_RANGE), value_idx(value_idx), ranges(ranges), num_ranges(num_ranges)
{
        VOLT_TRACE("HashRangeExpression %d %d", m_type, value_idx);
        for (int ii = 0; ii < num_ranges; ii++) {
            if (ii > 0) {
                if (ranges[ii - 1].first >= ranges[ii].first) {
                    throwFatalException("Ranges overlap or are out of order");
                }
                if (ranges[ii - 1].second > ranges[ii].first) {
                    throwFatalException("Ranges overlap or are out of order");
                }
            }
            if (ranges[ii].first > ranges[ii].second) {
                throwFatalException("Range begin is > range end, we don't support spanning Long.MAX to Long.MIN");
            }
        }
};

    virtual voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        vassert(tuple1);
        if ( ! tuple1 ) {
            throw SerializableEEException(
                    "TupleValueExpression::eval: Couldn't find tuple 1 (possible index scan planning error)");
        }
        const int32_t hash = tuple1->getNValue(this->value_idx).murmurHash3();

        return binarySearch(hash);
    }

    voltdb::NValue binarySearch(const int32_t hash) const {
        //The binary search blows up on only one range
        if (num_ranges == 1) {
            if (hash >= ranges[0].first && hash <= ranges[0].second) return NValue::getTrue();
            return NValue::getFalse();
        }

        /*
         * Bottom of a range is inclusive as well as the top. Necessary because we no longer support wrapping
         * from Integer.MIN_VALUE
         * Doing a binary search, is just a hair easier than std::lower_bound
         */
        int32_t min = 0;
        int32_t max = num_ranges - 1;
        while (min <= max) {
            vassert(min >= 0);
            vassert(max >= 0);
            uint32_t mid = (min + max) >> 1;
            if (ranges[mid].second < hash) {
                min = mid + 1;
            } else if (ranges[mid].first > hash) {
                max = mid - 1;
            } else {
                return NValue::getTrue();
            }
        }

        return NValue::getFalse();
    }

    std::string debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "Hash range expression on column[" << this->value_idx << "]\n";
        buffer << "ranges \n";
        for (int ii = 0; ii < num_ranges; ii++) {
            buffer << "start " << ranges[ii].first << " end " << ranges[ii].second << std::endl;
        }
        return (buffer.str());
    }

    int getColumnId() const {return this->value_idx;}

private:
    const int value_idx;           // which (offset) column of the tuple
    boost::scoped_array<srange_type> ranges;
    const int num_ranges;
};

}
#endif
