/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#ifndef VOLTDBHASHRANGEEXPRESSION_H
#define VOLTDBHASHRANGEEXPRESSION_H

#include "expressions/abstractexpression.h"
#include "common/tabletuple.h"

#include <string>
#include <sstream>

namespace voltdb {

const int HASH_RANGE_START = 0;
const int HASH_RANGE_END = 1;

class HashRangeExpression : public AbstractExpression {
  public:
	HashRangeExpression(int value_idx, int64_t ranges[][], int num_ranges )
        : AbstractExpression(EXPRESSION_TYPE_HASH_RANGE), value_idx(value_idx), ranges(ranges), num_ranges(num_ranges)
    {
        VOLT_TRACE("OptimizedTupleValueExpression %d %d", m_type, value_idx);
    };

    virtual voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
		assert(tuple1);
		if ( ! tuple1 ) {
			throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL,
										  "TupleValueExpression::"
										  "eval:"
										  " Couldn't find tuple 1 (possible index scan planning error)");
		}
		int64_t out[2];
		tuple1->getNValue(this->value_idx).murmurHash3(out);
		const int64_t hash = out[0];
		for (int ii = 0; ii < ranges; ii++) {
			if (ranges[ii][HASH_RANGE_START] <= hash && hash < ranges[ii][HASH_RANGE_END]) {
				return NValue::getTrue();
			} else if (hash < ranges[ii][HASH_RANGE_START]) {
				return NValue::getFalse();
			}
		}
		return NValue::getFalse();
    }

    std::string debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "Hash range expression on column[" << this->value_idx << "]\n";
        buffer << "ranges \n";
        for (int ii = 0; ii < num_ranges; ii++) {
        	buffer << "start " << ranges[ii][0] << " end " << ranges[ii][1] << std::endl;
        }
        return (buffer.str());
    }

    int getColumnId() const {return this->value_idx;}

  private:

    ~HashRangeExpression() {
    	delete(ranges);
    }

    const int value_idx;           // which (offset) column of the tuple
    const int num_ranges;
    const int64_t ranges[][];
};

}
#endif
