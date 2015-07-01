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

#ifndef FAILUREINJECTION_H_
#define FAILUREINJECTION_H_

#include <cstdlib>

#ifndef FAILURE_INJECTION_RATE
#define FAILURE_INJECTION_RATE .001
#endif

#ifndef INJECT_FAILURES
#define FAIL_IF( failureCondition ) FAIL_IFF( failureCondition, 1.0 )
#define FAIL_IFF( failureCondition, failureProbability ) \
    if (failureCondition)
#else
#define FAIL_IF( failureCondition ) FAIL_IFF( failureCondition, 1.0 )
#define FAIL_IFF( failureCondition, failureProbability ) \
 if (::rand() > (RAND_MAX * FAILURE_INJECTION_RATE * failureProbability) || failureCondition)
#endif


#endif /* FAILUREINJECTION_H_ */
