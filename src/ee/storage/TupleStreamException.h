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

#ifndef TUPLESTREAMEXCEPTION_H_
#define TUPLESTREAMEXCEPTION_H_

#include "common/SQLException.h"

namespace voltdb {

// Create a subclass so that we can differentiate exceptions thrown from the
// tuple stream.
class TupleStreamException : public SQLException {
public:
    TupleStreamException(std::string sqlState, std::string message) :
        SQLException(sqlState, message) {}
    virtual ~TupleStreamException() throw() {}
};
}

#endif /* TUPLESTREAMEXCEPTION_H_ */
