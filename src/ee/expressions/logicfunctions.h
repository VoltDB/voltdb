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

#include "common/NValue.hpp"

namespace voltdb {

/** implement the 2n/2n+1-argument DECODE function */
template<> inline NValue NValue::call<FUNC_DECODE>(const std::vector<NValue>& arguments) {
    int size = static_cast <int> (arguments.size());
    vassert(size >=3);
    int loopnum = ( size - 1 )/2;
    const NValue& baseval = arguments[0];
    for ( int i = 0; i < loopnum; i++ ) {
        const NValue& condval = arguments[2*i+1];
        if ( condval.compare(baseval) == VALUE_COMPARE_EQUAL ) {
            return arguments[2*i+2];
        }
    }
    const bool hasDefault = ( size % 2 == 0 );
    if ( hasDefault ) {
        NValue defaultResult = arguments[size-1];
        // See the comment above about the reason for un-inlining, here.
        if (defaultResult.getVolatile()) {
            defaultResult.allocateObjectFromPool();
        }
        return defaultResult;
    }
    return getNullValue();
}

/*
* Implement the Volt MIGRATING function.
* Returns true if the hidden column is NOT NULL, which means that the
* migrating process for this row has been started.
*/
template<> inline NValue NValue::callUnary<FUNC_VOLT_MIGRATING>() const {
    return ValueFactory::getBooleanValue(! isNull());
}
}
