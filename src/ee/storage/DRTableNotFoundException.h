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

#ifndef DRTABLENOTFOUNDEXCEPTION_H_
#define DRTABLENOTFOUNDEXCEPTION_H_

#include "common/SerializableEEException.h"

/*
 * Generated when a DR table cannot be found for table hash found in binary log.
 */
class DRTableNotFoundException : public voltdb::SerializableEEException {
public:
    DRTableNotFoundException(int64_t hash, std::string const& message);

    virtual std::string message() const;
    virtual ~DRTableNotFoundException() throw() { };

protected:
    void p_serialize(voltdb::ReferenceSerializeOutput *output) const;

    int64_t m_hash;
};

#endif /* DRTABLENOTFOUNDEXCEPTION_H_ */
