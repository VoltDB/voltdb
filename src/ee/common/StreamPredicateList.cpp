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

#include <string>
#include <limits>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "StreamPredicate.h"
#include "StreamPredicateList.h"

namespace voltdb
{

/*
 * Produce a list of StreamPredicateHashRange objects by parsing predicate range strings.
 * Add error messages to errmsg.
 * Return true on success.
 */
bool StreamPredicateList::parseStrings(
        const std::vector<std::string> &predicateStrings,
        std::ostringstream& errmsg)
{
    bool failed = false;
    std::size_t npreds = predicateStrings.size();
    for (std::size_t ipred = 0; ipred < npreds; ipred++) {
        std::string pred = predicateStrings.at(ipred);
        std::vector<std::string> snums;
        try {
            boost::split(snums, pred, boost::is_any_of(":"));
            if (snums.size() == 2) {
                int32_t minHash = boost::lexical_cast<int32_t>(snums[0]);
                int32_t maxHash = boost::lexical_cast<int32_t>(snums[1]);
                push_back(new StreamPredicate(minHash, maxHash));
            }
            else {
                errmsg << "Bad range predicate '" << pred << std::endl;
                failed = true;
            }
        }
        catch(std::exception &exc) {
            errmsg << "Failed to parse range predicate '" << pred
            << "': " << exc.what() << std::endl;
            failed = true;
        }
    }
    return !failed;
}

} // namespace voltdb
