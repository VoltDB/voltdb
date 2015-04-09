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

#ifndef GEOFUNCTIONS_H_
#define GEOFUNCTIONS_H_

#include "expressions/functionexpression.h"

namespace voltdb {

template<> NValue NValue::call<FUNC_VOLT_GEO_WITHIN>(const std::vector<NValue>& arguments);
template<> NValue NValue::callUnary<FUNC_VOLT_GEO_AREA>() const;
template<> NValue NValue::call<FUNC_VOLT_GEO_DISTANCE>(const std::vector<NValue>& arguments);
template<> NValue NValue::callUnary<FUNC_VOLT_GEO_NUM_POLYGONS>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_GEO_NUM_INTERIOR_RINGS>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_GEO_NUM_POINTS>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_GEO_PERIMETER>() const;

} // end namespace voltdb

#endif // GEOFUNCTIONS_H_
