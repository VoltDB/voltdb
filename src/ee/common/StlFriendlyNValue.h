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

#ifndef STLFRIENDLYNVALUE_H_
#define STLFRIENDLYNVALUE_H_

#include "common/NValue.hpp"

namespace voltdb {

/**
 * Add a thin layer of stl-friendly behavior -- but no additional data members -- to NValue.
 * Rationale: NValue shies away from heavy use of operator overloading, etc. and for some good reasons
 * (IMHO --paul).
 * But it's these kinds of things that grease the wheels for using NValues in stl containers,
 * so as needed for their sake, construct or cast the NValue references to StlFriendlyNValue references,
 * preferably in a way that avoids copying.
 **/
struct StlFriendlyNValue : public NValue {
    // StlFriendlyNValue instances need to be used within STL containers that support
    // methods or algorithms that require operator== support (& possibly other methods)
    // Only the minimal set of NValue constuctors (default, copy)
    // actually need be exposed through StlFriendlyNValue.

    bool operator==(const StlFriendlyNValue& other) const
    {
        return compare(other) == VALUE_COMPARE_EQUAL;
    }

    const StlFriendlyNValue& operator=(const NValue& other)
    {
        // Just call "super".
        (*(NValue*)this) = other;
        return *this;
    }

    bool operator<(const StlFriendlyNValue& other) const
    {
        return compare(other) < VALUE_COMPARE_EQUAL;
    }

};

}

#endif // STLFRIENDLYNVALUE_H_

