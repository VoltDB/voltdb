/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

namespace voltdb {

/** implement the 2n/2n+1-argument DECODE function */
template<> inline NValue NValue::call<FUNC_DECODE>(const std::vector<NValue>& arguments) {
    int size = (int)arguments.size();
    assert(size>=3);
    NValue condval;
    int loopnum = ( size - 1 )/2;
    NValue baseval = arguments[0];
    for( int i = 0; i < loopnum; i++ ) {
        condval = arguments[2*i+1];
        if( condval.compare(baseval) == VALUE_COMPARE_EQUAL ) {
            return arguments[2*i+2];
        }
    }
    bool hasDefault = ( size % 2 == 0 );
    if( hasDefault ) {
        return arguments[size-1];
    }
    return getNullValue();
}

}
