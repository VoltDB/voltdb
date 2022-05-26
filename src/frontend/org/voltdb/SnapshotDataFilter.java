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
package org.voltdb;

import java.util.concurrent.Callable;

import org.voltcore.utils.DBBPool.BBContainer;

/*
 * Interface for a class that accepts a block of raw tuples from the EE
 * and converts the rows and re-emits them or swallows (filters) them.
 */
public interface SnapshotDataFilter {
    public Callable<BBContainer> filter(Callable<BBContainer> cont);
}
