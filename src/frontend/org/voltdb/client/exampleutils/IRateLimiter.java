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
package org.voltdb.client.exampleutils;

/**
 * Defines the generic interface for a transaction rate limiter.
 * Rate limiters are useful for asynchronous applications that want to ensure
 * a certain level of overall system performance, whether by targetting a
 * specific execution latency or maximum throughput to prevent fire-hosing
 * which can have negative performance impact.
 *
 * @author Seb Coursol
 * @since 2.0
 */
public interface IRateLimiter
{
    /**
     * Throttle the execution process by forcing the thread to sleep as necessary to ensure the desired rate is achieved.
     * This method should be called after each asynchronous execution call so it can count requested executions versus elapsed time and requested target rate/latency to decide whether to return immediately or sleep.
     */
    void throttle();
}

