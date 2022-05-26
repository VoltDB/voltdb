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

#ifndef GEOFUNCTIONS_H
#define GEOFUNCTIONS_H

#include "common/NValue.hpp"
#include "expressions/functionexpression.h"

namespace voltdb {
template<> NValue NValue::callUnary<FUNC_VOLT_POINTFROMTEXT>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_POLYGONFROMTEXT>() const;
template<> NValue NValue::call<FUNC_VOLT_CONTAINS>(const std::vector<NValue>& arguments);
template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_NUM_POINTS>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_POINT_LATITUDE>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_POINT_LONGITUDE>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_CENTROID>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_POLYGON_AREA>() const;
template<> NValue NValue::call<FUNC_VOLT_DISTANCE_POINT_POINT>(const std::vector<NValue>& arguments);
template<> NValue NValue::call<FUNC_VOLT_DISTANCE_POLYGON_POINT>(const std::vector<NValue>& arguments);
template<> NValue NValue::callUnary<FUNC_VOLT_ASTEXT_GEOGRAPHY_POINT>() const;
template<> NValue NValue::callUnary<FUNC_VOLT_ASTEXT_GEOGRAPHY>() const;
template<> NValue NValue::call<FUNC_VOLT_DWITHIN_POLYGON_POINT>(const std::vector<NValue>& arguments);
template<> NValue NValue::call<FUNC_VOLT_DWITHIN_POINT_POINT>(const std::vector<NValue>& arguments);
}

#endif
