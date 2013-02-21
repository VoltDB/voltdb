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

#ifndef STREAMPREDICATELIST_H_
#define STREAMPREDICATELIST_H_

#include <boost/shared_ptr.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

namespace voltdb
{

/**
 * A templated list (vector) of predicates. The template assures homogeneity of
 * predicate class so that the predicates can be checked for consistency and
 * completeness.
 */
template <typename TStreamPredicate>
class StreamPredicateList : public boost::ptr_vector<TStreamPredicate>
{
public:

    /** Default vconstructor. */
    StreamPredicateList() : boost::ptr_vector<TStreamPredicate>()
    {}

    /** Constructor with reserved size. */
    StreamPredicateList(std::size_t size) : boost::ptr_vector<TStreamPredicate>(size)
    {}

    /** Destructor. */
    virtual ~StreamPredicateList()
    {}

    /**
     * Factory method to parse a bunch of predicate strings and return a vector
     * of TStreamPredicate objects. Also sanity checks the sequence of
     * predicates for completeness, etc..
     */
    static boost::shared_ptr<StreamPredicateList<TStreamPredicate> > parse(
            const std::vector<std::string>& predicate_strings)
    {
        std::ostringstream errmsg;
        boost::shared_ptr<StreamPredicateList<TStreamPredicate> > predicates_out(
                new StreamPredicateList<TStreamPredicate>(predicate_strings.size()));
        if (!TStreamPredicate::parse(predicate_strings, *predicates_out, errmsg)) {
            predicates_out->clear();
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, errmsg.str());
        }
        return predicates_out;
    }
};

} // namespace voltdb

#endif /* defined(STREAMPREDICATELIST_H_) */
