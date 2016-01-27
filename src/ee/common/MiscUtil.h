/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef MISC_UTIL_H_
#define MISC_UTIL_H_

#include <string>
#include <vector>

#include "boost/functional/hash.hpp"

namespace voltdb
{

/**
 * Miscellaneous static utility methods.
 */
class MiscUtil
{

  public:

    /**
     * Split string on delimiter into any number of sub-strings.
     */
    static std::vector<std::string> splitString(const std::string &str, char delimiter);

    /**
     * Split string on delimiter into two sub-strings.
     */
    static std::vector<std::string> splitToTwoString(const std::string &str, char delimiter);

    /**
     * A hashCombine function that can deal with the quirks of floating point math
     * on the various platforms that we support.
     */
    static void hashCombineFloatingPoint(std::size_t &seed, double value) {
        // This method was observed to fail on Centos 5 / GCC 4.1.2, returning different hashes
        // for identical inputs, so the conditional was added,
        // mutated from the one in boost/type_traits/intrinsics.hpp,
        // and the broken overload for "double" was by-passed in favor of the more reliable
        // one for int64 -- even if this may give sub-optimal hashes for typical collections of double.
        // This conditional can be dropped when Centos 5 support is dropped.
#if defined(__GNUC__) && ((__GNUC__ > 4) || ((__GNUC__ == 4) && (__GNUC_MINOR__ >= 2) && !defined(__GCCXML__))) && !defined(BOOST_CLANG)
        boost::hash_combine( seed, value);
#else
        {
            const int64_t proxyForDouble =  *reinterpret_cast<const int64_t*>(&value);
            boost::hash_combine( seed, proxyForDouble);
        }
#endif
    }
};

} // namespace voltdb

#endif // MISC_UTIL_H_
