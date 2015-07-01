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

package org.voltdb;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotates a stored procedure with information needed by the stored
 * procedure compiler.</p>
 *
 * <p>This is not Java deprecated, but this information is
 * commonly provided in DDL rather than using this annotation. The compiler
 * will complain if this annotation and DDL partitioning info is provided
 * for the same procedure, even if it doesn't conflict.</p>
 *
 * @see VoltProcedure
 * @see ProcInfoData
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProcInfo {

    /**
     * Information needed to direct this procedure call to the proper partition(s).
     * @return A string of the form "table.column: parametername" that maps columns
     * in a table to a parameter value. This is required for a single-sited procedure.
     */
    String partitionInfo() default "";

    /**
     * Is the procedure meant for a single partition?
     * @return True if all statements run on the same partition always, false otherwise.
     */
    boolean singlePartition() default false;
}
