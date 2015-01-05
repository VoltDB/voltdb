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

#ifndef CONSTRAINTFAILUREEXCEPTION_H_
#define CONSTRAINTFAILUREEXCEPTION_H_

#include "common/SQLException.h"
#include "common/types.h"
#include "common/ids.h"
#include "common/tabletuple.h"

#include <string>

namespace voltdb {
class PersistentTable;
class VoltDBEngine;

/*
 * A constraint exception is generated when an update or an insert on a table violates a constraint
 */
class ConstraintFailureException: public SQLException {
public:
    /**
     * General constructor for for CFE
     *
     * @param table Table that the update or insert was performed on
     * @param tuple Tuple that was being inserted or updated
     * @param otherTuple updated tuple values or a null tuple.
     * @param type Type of constraint that was violated
     */
    ConstraintFailureException(PersistentTable *table, TableTuple tuple, TableTuple otherTuple, ConstraintType type);

    /**
     * Special constructor for partitioning error CFEs only
     *
     * @param table Table that the update or insert was performed on
     * @param tuple Tuple that was being inserted or updated
     * @param message Description of the partitioning failure.
     */
    ConstraintFailureException(PersistentTable *table, TableTuple tuple, std::string message);

    virtual const std::string message() const;
    virtual ~ConstraintFailureException();
protected:
    void p_serialize(ReferenceSerializeOutput *output) const;

    PersistentTable *m_table;
    TableTuple m_tuple;
    TableTuple m_otherTuple;
    ConstraintType m_type;
};

}

#endif /* CONSTRAINTFAILUREEXCEPTION_H_ */
