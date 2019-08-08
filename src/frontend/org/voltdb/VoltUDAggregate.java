/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb;

import org.voltdb.VoltType;

public interface VoltUDAggregate<T, U> {

    /**
     * The customer needs to define their own data strucutes
     * to store the aggregation results.
     * This method initializes the data structures.
     */
    public void start();

    /**
     * Add a value of column type T to the aggregation results.
     */
    public void assemble(T val);

    /**
     * Combine the partial results of different tables together (partitioned)
     * @param other An instance of the implementing class of interface VoltUDAggregate
     */
    public void combine(U other);

    /**
     * @return return the aggregation results based on the used defined return value U
     */
    public Object end();
}
