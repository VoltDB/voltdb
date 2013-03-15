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

#ifndef ELASTICHASHINATOR_H_
#define ELASTICHASHINATOR_H_

#include "common/FatalException.hpp"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/serializeio.h"
#include "common/TheHashinator.h"
#include "boost_ext/FastAllocator.hpp"

#include <cstring>
#include <string>
#include <cassert>
#include <stdlib.h>
#include <stx/btree_map>
#include <murmur3/MurmurHash3.h>
#include <limits>

/*
 * Forward declaration for test friendship
 */
class ElasticHashinatorTest_TestMinMaxToken;

namespace voltdb {


/*
 * Concrete implementation of TheHashinator that uses MurmurHash3_x64_128 to hash values
 * onto a consistent hash ring.
 */
class ElasticHashinator : public TheHashinator {
    friend class ::ElasticHashinatorTest_TestMinMaxToken;
public:

    /*
     * Factory method that constructs an ElasticHashinator from a binary configuration.
     * The format is described in ElasticHashinator.java and basically describes the tokens
     * on the ring.
     */
    static ElasticHashinator* newInstance(const char *config) {
        ReferenceSerializeInput countInput(config, 4);
        int numEntries = countInput.readInt();
        ReferenceSerializeInput entryInput(&config[sizeof(int32_t)], numEntries * (sizeof(int32_t) * sizeof(int64_t)));
        TokenMap tokens;
        for (int ii = 0; ii < numEntries; ii++) {
            const int64_t token = entryInput.readLong();
            const int32_t partitionId = entryInput.readInt();
            if (tokens.exists(token)) {
                throwFatalException("Duplicate token in ring, %jd with partitions %d and %d",
                        (intmax_t)token, partitionId, tokens[token]);
            }
            tokens[token] = partitionId;
        }
        return new ElasticHashinator(tokens);
    }

    ~ElasticHashinator() {}
protected:

    /**
     * Given a long value, pick a partition to store the data.
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    int32_t hashinate(int64_t value) const {
        // special case this hard to hash value to 0 (in both c++ and java)
        if (value == INT64_MIN) return 0;

        return partitionForToken(MurmurHash3_x64_128(value));
    }

    /*
     * Given a piece of UTF-8 encoded character data OR binary data
     * pick a partition to store the data
     */
    int32_t hashinate(const char *string, int32_t length) const {
        int64_t out[2];
        MurmurHash3_x64_128(string, length, 0, out);
        return partitionForToken(out[0]);
    }

private:
    typedef stx::btree_map<int64_t, int32_t> TokenMap;
    ElasticHashinator(TokenMap tokenMap) : tokens(tokenMap) {}

    int32_t partitionForToken(int64_t hash) const {
        /*
         * C++ doesn't have the NavigableMap equivalent of floor,
         * so we use upper_bound and step back, except if upper bound
         * returns tokens.begin()
         */
        TokenMap::const_iterator i = tokens.upper_bound(hash);
        //std::cout << "EE finding partition for token " << token << std::endl;

        /*
         * Hash value is < the smallest token (tokens.begin()) in which
         * case it actually maps to the last/largest token since conceptually this is a ring.
         */
        if (i == tokens.begin()) {
            return tokens.rbegin().data();
        }

        /*
         * Move the iterator back one, this is upper bound, so to find the floor we have to find the first
         * value that did not come after the hash value. There are test cases that show if upper_bound
         * returns tokens.end() that it moves back to tokens.rbegin() on its own.
         */
        i--;

        /*
         * At this point we will have the right value, and bounds should not be an issue
         * due to the checks for tokens.begin. tokens.end is handled by always moving the iterator back one
         */
        return i.data();
    }

    const TokenMap tokens;
};
}
#endif /* ELASTICHASHINATOR_H_ */
