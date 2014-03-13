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

namespace voltdb {

/** implement the 2n/2n+1-argument DECODE function */
template<> inline NValue NValue::call<FUNC_DECODE>(const std::vector<NValue>& arguments) {
    int size = (int)arguments.size();
    assert(size>=3);
    int loopnum = ( size - 1 )/2;
    const NValue& baseval = arguments[0];
    for ( int i = 0; i < loopnum; i++ ) {
        const NValue& condval = arguments[2*i+1];
        if ( condval.compare(baseval) == VALUE_COMPARE_EQUAL ) {
            NValue result = arguments[2*i+2];
            // The un-inlining of persistent table columns done here is actually only required when
            // the result value is used for a temp table column (typically non-inlined to accomodate
            // any object length).
            // This allocation is wasted in cases where the result is "consumed" by another expression.
            // It would probably make more sense for assignment of temp table tuple columns from NValues
            // to do the allocation as needed -- that would only require the tabletuple to be aware of
            // its table context (persistent or temp) so it would pick the right allocator.
            // This would explode the myth of the abstract API for write access to persistent and temp
            // tables.  Streamed (i.e. export) tables still need to share an abstract API for write
            // access with persistent tables -- which is OK because they both use the persistent object
            // allocator/pool?.
            // An alternative approach would be for the planner to inject an explicit "allocate"
            // operation expression only as needed at the top of any (output schema? projection?)
            // expression tree that had the possibility of passing through a tuple value untouched from
            // a narrowly sized variable length column all the way to a potentially wide one.
            if ( result.m_sourceInlined ) {
                result.allocateObjectFromInlinedValue();
            }
            return result;
        }
    }
    const bool hasDefault = ( size % 2 == 0 );
    if ( hasDefault ) {
        NValue defaultResult = arguments[size-1];
        // See the comment above about the reason for un-inlining, here.
        if ( defaultResult.m_sourceInlined ) {
            defaultResult.allocateObjectFromInlinedValue();
        }
        return defaultResult;
    }
    return getNullValue();
}

}
