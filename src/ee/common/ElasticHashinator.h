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

#include "common/TheHashinator.h"
#include "common/serializeio.h"

//#include <cstring>
//#include <string>
//#include <cassert>
//#include <stdlib.h>
#include <murmur3/MurmurHash3.h>
//#include <limits>

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
     * The raw version consists of an array of integers where even-indexed
     * values are tokens and odd-indexed values are partition ids.
     */
    static ElasticHashinator* newInstance(const char *config, int32_t *configPtr, uint32_t tokenCount) {
        bool ownConfig = (configPtr == NULL);
        if (ownConfig) {
            ReferenceSerializeInputBE countInput(config, 4);
            tokenCount = countInput.readInt();
            configPtr = new int32_t[tokenCount * 2];
            ReferenceSerializeInputBE entryInput(config + sizeof(int32_t), tokenCount * (sizeof(int32_t) + sizeof(int32_t)));
            for (int ii = 0; ii < tokenCount; ii++) {
                const int32_t token = entryInput.readInt();
                const int32_t partitionId = entryInput.readInt();
                configPtr[ii * 2] = token;
                configPtr[ii * 2 + 1] = partitionId;
            }
        }
        return new ElasticHashinator(configPtr, tokenCount, ownConfig);
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

    ElasticHashinator(const int32_t* tokens, uint32_t tokenCount, bool owned)
      : m_tokens(tokens)
      , m_tokenCount(tokenCount)
      , m_tokensOwner( owned ? tokens : NULL )
    {}

    const int32_t* const m_tokens;
    const uint32_t m_tokenCount;
    const boost::scoped_array<const int32_t> m_tokensOwner;

    int32_t tokenAt(int32_t index) const { return m_tokens[index * 2]; }
    int32_t partitionIdAt(int32_t index) const { return m_tokens[index * 2 + 1]; }

    int32_t partitionForToken(int32_t hash) const {
        int32_t min = 0;
        int32_t max = m_tokenCount - 1;

        while (min <= max) {
            assert(min >= 0);
            assert(max >= 0);
            uint32_t mid = (min + max) >> 1;
            int32_t midval = tokenAt(mid);

            if (midval < hash) {
                min = mid + 1;
            }
            else if (midval > hash) {
                max = mid - 1;
            }
            else {
                return partitionIdAt(mid);
            }
        }
        return partitionIdAt(min - 1);
    }
};
}
#endif /* ELASTICHASHINATOR_H_ */
