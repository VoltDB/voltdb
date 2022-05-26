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
#pragma once

#include "boost/date_time/gregorian/greg_date.hpp"
#include "boost/date_time/posix_time/posix_time_types.hpp"
#include "boost/date_time/posix_time/posix_time_duration.hpp"
#include "boost/date_time/posix_time/ptime.hpp"
#include "boost/date_time/posix_time/conversion.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "boost/date_time/gregorian/gregorian.hpp"

/*
 * Some date and time constants needed in datafunctions
 * and also other places.
 */
static const boost::posix_time::ptime EPOCH(boost::gregorian::date(1970,1,1));
static const boost::gregorian::date EPOCH_DATE(boost::gregorian::date(1970,1,1));
static const int64_t GREGORIAN_EPOCH = -12212553600000000;  //  1583-01-01 00:00:00
static const int64_t NYE9999         = 253402300799999999;  //  9999-12-31 23:59:59.999999

