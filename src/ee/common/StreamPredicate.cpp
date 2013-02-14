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

#include "StreamPredicate.h"
#include "tabletuple.h"
#include "storage/persistenttable.h"
#include "TheHashinator.h"
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace voltdb {

// Parse ("<min>-<max>") ranges out of the predicate_strings strings Capture
// error messages in errmsg (separated by \n) and check at the end to see if an
// exception must be thrown. predicates_out is cleared before parsing.
void StreamPredicate::parse(
        const std::vector<std::string> &predicate_strings,
        StreamPredicateList &predicates_out) {

    // Collects error messages. Checked later to see if any errors occurred.
    std::ostringstream errmsg;

    predicates_out.clear();

    for (std::vector<std::string>::const_iterator predi = predicate_strings.begin(),
                                                      e = predicate_strings.end();
         predi != e; ++predi) {
        std::vector<std::string> snums;
        try {
            boost::split(snums, *predi, boost::is_any_of("-"));
            if (snums.size() == 2) {
                size_t minHash = boost::lexical_cast<size_t>(snums[0]);
                size_t maxHash = boost::lexical_cast<size_t>(snums[1]);
                if (predicates_out.empty()) {
                    if (minHash != 0) {
                        errmsg << "First min hash, " << minHash << ", is non-zero"
                        << " for range predicate '" << *predi << "'" << std::endl;
                    }
                }
                else {
                    size_t prevMaxHash = predicates_out.back().m_maxHash;
                    if (minHash != prevMaxHash + 1) {
                        errmsg << "Min hash " << minHash
                        << " is not previous max (" << prevMaxHash << ") + 1"
                        << " for range predicate '" << *predi << "'" << std::endl;
                    }
                }
                if (maxHash <= minHash) {
                    errmsg << "Max <= min for range predicate '" << *predi << "'" << std::endl;
                }
                if (!errmsg.str().empty()) {
                    predicates_out.push_back(new StreamPredicate(minHash, maxHash));
                }
            }
            else {
                errmsg << "Bad range predicate '" << *predi << std::endl;
            }
        }
        catch(std::exception &exc) {
            errmsg << "Failed to parse range predicate '" << *predi
            << "': " << exc.what() << std::endl;
        }
    }
    // Handle failures with an exception.
    if (!errmsg.str().empty()) {
        // Clean up.
        predicates_out.clear();
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, errmsg.str());
    }
}

bool StreamPredicate::accept(
        PersistentTable &table,
        const TableTuple &tuple,
        int32_t totalPartitions) const {
    int partitionColumn = table.partitionColumn();
    if (partitionColumn == -1) {
        return true;
    }
    NValue value = tuple.getNValue(partitionColumn);
    int hash = TheHashinator::hashinate(value, totalPartitions);
    return (hash >= m_minHash && hash <= m_maxHash);
}

} // namespace voltdb
