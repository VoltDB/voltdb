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
#ifndef TUPLEITERATOR_H_
#define TUPLEITERATOR_H_
#include "common/tabletuple.h"

/**
 * Interface for iterators that return tuples.
 */
namespace voltdb {
class TupleIterator {
public:
    virtual bool next(TableTuple &out) = 0;
    virtual ~TupleIterator() {}
};
}

#endif /* TUPLEITERATOR_H_ */
