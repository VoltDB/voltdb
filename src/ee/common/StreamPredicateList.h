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

#ifndef STREAMPREDICATELIST_H_
#define STREAMPREDICATELIST_H_

#include <boost/shared_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include "SerializableEEException.h"
#include "expressions/abstractexpression.h"

namespace voltdb
{

/**
 * A list (vector) of predicates.
 */
class StreamPredicateList : public boost::ptr_vector< boost::nullable<AbstractExpression> >
{
public:

    /** Default vconstructor. */
    StreamPredicateList() : boost::ptr_vector< boost::nullable<AbstractExpression> >()
    {}

    /** Constructor with reserved size. */
    StreamPredicateList(std::size_t size) : boost::ptr_vector< boost::nullable<AbstractExpression> >(size)
    {}

    /** Destructor. */
    virtual ~StreamPredicateList()
    {}

    /** Parse expression strings and add generated predicate objects to list. */
    bool parseStrings(const std::vector<std::string> &predicateStrings,
                      std::ostringstream& errmsg,
                      std::vector<bool> &predicateDeleteFlags);
};

} // namespace voltdb

#endif /* defined(STREAMPREDICATELIST_H_) */
