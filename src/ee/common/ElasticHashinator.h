/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
     *
     * Config can be serialized or raw, if it is raw the pointer is stored and the data
     * can be shared across EEs and with Java.
     * The raw version consists of an array of integers where even values are tokens and odd values are partitions.
     *
     */
    static ElasticHashinator* newInstance(const char *config, int32_t *configPtr, uint32_t tokenCount) {
        if (configPtr == NULL) {
            ReferenceSerializeInputBE countInput(config, 4);
            int numEntries = countInput.readInt();
            ReferenceSerializeInputBE entryInput(&config[sizeof(int32_t)], numEntries * (sizeof(int32_t) + sizeof(int32_t)));
            int32_t *tokens = new int32_t[numEntries * 2];
            for (int ii = 0; ii < numEntries; ii++) {
                const int32_t token = entryInput.readInt();
                const int32_t partitionId = entryInput.readInt();
                tokens[ii * 2] = token;
                tokens[ii * 2 + 1] = partitionId;
            }
            return new ElasticHashinator(tokens, numEntries, true);
        } else {
            return new ElasticHashinator(configPtr, tokenCount, false);
        }
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
        int32_t hash = MurmurHash3_x64_128(string, length, 0);
        return partitionForToken(hash);
    }

private:

    ElasticHashinator(int32_t *tokens, uint32_t tokenCount, bool owned) : tokens(tokens), tokenCount(tokenCount), tokensOwner( owned ? tokens : NULL ) {}

    const int32_t *tokens;
    const uint32_t tokenCount;
    boost::scoped_array<int32_t> tokensOwner;

    int32_t partitionForToken(int32_t hash) const {
        int32_t min = 0;
        int32_t max = tokenCount - 1;

        while (min <= max) {
            assert(min >= 0);
            assert(max >= 0);
            uint32_t mid = (min + max) >> 1;
            int32_t midval = tokens[mid * 2];

            if (midval < hash) {
                min = mid + 1;
            } else if (midval > hash) {
                max = mid - 1;
            } else {
                return tokens[mid * 2 + 1];
            }
        }
        return tokens[(min - 1) * 2 + 1];
    }
};
}
#endif /* ELASTICHASHINATOR_H_ */
