/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef VOLTDB_TUPLEBLOCK_H_
#define VOLTDB_TUPLEBLOCK_H_
#include <vector>
#include <stdint.h>
#include <string.h>
#include "boost/scoped_array.hpp"
#include "boost/intrusive_ptr.hpp"
#include "boost/shared_ptr.hpp"
#include "stx/btree_map.h"

namespace voltdb {
class TupleBlock;
}

namespace boost {
    void intrusive_ptr_add_ref(voltdb::TupleBlock * p);
    void intrusive_ptr_release(voltdb::TupleBlock * p);
};

namespace voltdb {
class Table;

class TruncatedInt {
public:
    TruncatedInt(uint32_t value) {
        ::memcpy(m_data, reinterpret_cast<char*>(&value), 3);
    }

    TruncatedInt(const TruncatedInt &other) {
        ::memcpy(m_data, other.m_data, 3);
    }

    TruncatedInt& operator=(const TruncatedInt&rhs) {
        ::memcpy(m_data, rhs.m_data, 3);
        return *this;
    }

    uint32_t unpack() {
        char valueBytes[4];
        ::memcpy(valueBytes, m_data, 3);
        valueBytes[3] = 0;
        return *reinterpret_cast<int32_t*>(valueBytes);
    }
private:
    char m_data[3];
};

class TupleBlock {
    friend void ::boost::intrusive_ptr_add_ref(voltdb::TupleBlock * p);
    friend void ::boost::intrusive_ptr_release(voltdb::TupleBlock * p);
public:
    TupleBlock(Table *table);

     double loadFactor();

     bool hasFreeTuples();

     bool isEmpty();

     char* nextFreeTuple();

     void freeTuple(char *tupleStorage);

     char * address();

     void reset();

     uint32_t unusedTupleBoundry();
//
//    inline double loadFactor() {
//        return m_activeTuples / static_cast<double>(m_tuplesPerBlock);
//    }
//
//    inline bool hasFreeTuples() {
//        return m_activeTuples < m_tuplesPerBlock;
//    }
//
//    inline bool isEmpty() {
//        if (m_activeTuples == 0) {
//            return false;
//        }
//        return true;
//    }
//
//    inline char* nextFreeTuple() {
//        char *retval = NULL;
//        if (!m_freeList.empty()) {
//            char *offsetCompressed = &m_freeList[m_freeList.size() - 3];
//            char offsetBytes[4];
//            ::memcpy(offsetBytes, offsetCompressed, 3);
//            offsetBytes[3] = 0;
//            if (m_freeList.capacity() / 2 > m_freeList.size()) {
//                std::vector<char>(m_freeList).swap(m_freeList);
//            }
//            retval = *reinterpret_cast<char**>(offsetBytes);
//            m_freeList.pop_back();
//            m_freeList.pop_back();
//            m_freeList.pop_back();
//        } else {
//            retval = &(m_storage.get()[m_tupleLength * m_nextFreeTuple]);
//            m_nextFreeTuple++;
//        }
//        m_activeTuples++;
//        return retval;
//    }
//
//    inline void freeTuple(char *tupleStorage) {
//        m_activeTuples--;
//        //Find the offset
//        uint32_t offset = static_cast<uint32_t>((tupleStorage - m_storage.get()) / m_tupleLength);
//        char compressedOffset[3];
//        ::memcpy( compressedOffset, reinterpret_cast<char*>(&offset), 3);
//        m_freeList.push_back(compressedOffset[0]);
//        m_freeList.push_back(compressedOffset[1]);
//        m_freeList.push_back(compressedOffset[2]);
//    }
//
//    inline char * address() {
//        return m_storage.get();
//    }
//
//    inline void reset() {
//        m_activeTuples = 0;
//        m_nextFreeTuple = 0;
//        m_freeList.clear();
//    }
//
//    inline uint32_t unusedTupleBoundry() {
//        return m_nextFreeTuple;
//    }

private:
    uint32_t m_references;
    Table* m_table;
    boost::scoped_array<char>  m_storage;
    uint32_t m_tupleLength;
    uint32_t m_tuplesPerBlock;
    uint32_t m_activeTuples;
    uint32_t m_nextFreeTuple;
    /*
     * queue of offsets to <b>once used and then deleted</b> tuples.
     * Tuples after m_nextFreeTuple are also free, this queue
     * is used to find "hole" tuples which were once used (before used_tuples index)
     * and also deleted.
     * NOTE THAT THESE ARE NOT THE ONLY FREE TUPLES.
     **/
    std::vector<TruncatedInt> m_freeList;
};

//typedef boost::shared_ptr<TupleBlock> TBPtr;
typedef boost::intrusive_ptr<TupleBlock> TBPtr;
//typedef TupleBlock* TBPtr;
typedef stx::btree_map< char*, TBPtr > TBMap;
typedef TBMap::iterator TBMapI;
}

namespace boost
{
 inline void intrusive_ptr_add_ref(voltdb::TupleBlock * p)
  {
    ++(p->m_references);
  }



 inline void intrusive_ptr_release(voltdb::TupleBlock * p)
  {
   if (--(p->m_references) == 0)
     delete p;
  }
}

#endif /* VOLTDB_TUPLEBLOCK_H_ */
